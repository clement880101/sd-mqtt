//! Wiring the configured rules into a running broker.
//!
//! **Most of what used to be here is now [`rules::live`].** It was here
//! because this was the only place holding a store, a writer and a
//! broker at once; section 11.1's admin API is a second place that
//! needs all three, and the admin crate cannot see this one. So the
//! registry moved down into the rules crate and what is left here is
//! the configuration file's half: read the `[[rules]]` tables, install
//! each one, install the hook, and say what the store holds that the
//! file no longer names.
//!
//! # Rules are written into the store at startup
//!
//! A rule declared in the configuration is put into the store before
//! the broker serves anything, because an outbox hangs off a rule and
//! the store refuses an envelope for a rule it does not hold. The put
//! is idempotent — a rule is whatever the last put said it was — so a
//! restart with an unchanged file changes nothing, and a restart with
//! an edited one applies the edit **without touching the outbox**,
//! which is what makes turning a connector off and on again safe.
//!
//! **And a rule created over the API survives the file.** The store is
//! read back below and anything in it the file does not name is
//! installed too, which is what makes a rule created over the API fire
//! again after a restart. Before that existed there was no way to
//! write a rule but the file, so a stored rule the file had dropped
//! could only be an edit somebody had made, and the safe thing was to
//! leave it alone; now it is usually a rule somebody created over the
//! API and the safe thing is to run it. A rule an operator wants gone
//! is deleted, which is a thing the API can now do.
//!
//! # A singleton is started here, and asks whether it may run
//!
//! Section 9.2 makes a poller a singleton owned by the live member with
//! the lowest node identifier. This node is still the only member, so
//! the [`Membership`] built below holds one node and that node owns
//! everything — but it is a membership view and the poller asks it,
//! rather than a poller written on the assumption that it is alone. The
//! reasoning is `rules::singleton`'s; what remains for the rest of
//! milestone 5 is to hand that view the peers it learns about, and
//! nothing else here moves.
//!
//! The member it holds is **section 8.1's stored identity**, handed in
//! by the caller, rather than a constant this file used to carry.

use std::collections::BTreeSet;
use std::sync::Arc;

use rules::live::Registry;
use rules::rule::Rule;
use rules::singleton::{Membership, NodeId};
use rules::Ingest;
use sdmqtt_server::shared::Shared;
use storage::{Storage, Writer};

use crate::config::Config;
use crate::StartupError;

/// The rules this node is running, and the routes they serve.
pub struct Connectors {
    /// `None` on a node with no store. Section 9.1 makes a rule
    /// durable metadata, and `Config::validate` already refuses a
    /// configuration that declares one with nowhere to put it; what
    /// this carries is that the admin API has no rules to serve
    /// either, which it says rather than answering with an empty
    /// collection.
    registry: Option<Arc<Registry>>,
    /// The ingest routes, which are the registry's when there is one
    /// and empty when there is not.
    ingest: Arc<Ingest>,
}

impl Connectors {
    /// How many rules are installed.
    pub fn len(&self) -> usize {
        self.registry.as_ref().map_or(0, |registry| registry.len())
    }

    /// The ingest rules this node serves.
    pub fn ingest(&self) -> &Arc<Ingest> {
        &self.ingest
    }

    /// The registry, for whoever serves it over the API.
    pub fn registry(&self) -> Option<&Arc<Registry>> {
        self.registry.as_ref()
    }

    /// Stop every pump.
    pub async fn shutdown(self) {
        if let Some(registry) = self.registry {
            registry.shutdown().await;
        }
    }
}

/// No rules, because there is no store to hold one.
///
/// Its own function rather than an `Option<Connectors>`, because what
/// the caller does with it afterwards is identical.
pub fn start_without_rules() -> Connectors {
    Connectors {
        registry: None,
        ingest: Arc::new(Ingest::new()),
    }
}

/// Install the configured rules and start their pumps.
///
/// Called after the store has been read and before any listener binds,
/// which is the order that matters: a client cannot publish into a
/// broker whose rules are half installed.
///
/// # Errors
///
/// [`StartupError::Connector`] when a rule cannot be stored. A rule that
/// cannot be *used* was refused by `Config::validate` long before this.
pub async fn start(
    config: &Config,
    shared: &Arc<Shared>,
    store: &Arc<dyn Storage>,
    writer: &Arc<Writer>,
    this_node: NodeId,
) -> Result<Connectors, StartupError> {
    // One member, so it owns every singleton. Written as a view
    // because a rule only ever exercised in the one-member case is a
    // rule nobody has tested; see `rules::singleton`.
    //
    // **The member is this node's stored identity**, section 8.1's,
    // read or minted before this is called. It used to be the constant
    // `"local"`, which was the second answer to "which node" this
    // milestone existed to remove: a cluster of one whose member was a
    // literal would have joined a cluster of three under a name it
    // shared with both of them.
    let membership = Arc::new(Membership::alone(this_node));
    let registry = Registry::new(
        Arc::clone(shared),
        Arc::clone(store),
        Arc::clone(writer),
        membership,
    );

    let mut configured = BTreeSet::new();
    for section in &config.rules {
        // `Config::validate` has already refused anything that cannot
        // be used, so this cannot fail. Going through the same
        // function rather than a second construction of the rule is
        // what keeps that true.
        let rule = section
            .to_rule()
            .map_err(|error| StartupError::Connector(error.why))?;
        configured.insert(rule.id.as_str().to_owned());
        registry
            .install(rule)
            .await
            .map_err(|error| StartupError::Connector(error.to_string()))?;
    }

    // A rule the store holds that the configuration does not name.
    // Installed, because a rule created over the API is exactly that
    // and it has to fire again after a restart; see the module note.
    let held = store
        .restore()
        .map_err(|error| StartupError::Connector(error.to_string()))?
        .rules;
    for record in held.iter().filter(|held| !configured.contains(&held.id)) {
        // Read back through the same conversion a rule from a file
        // goes through, so a record this build will not accept is a
        // line in the log rather than a rule running on assumptions
        // nothing checked.
        let rule = match Rule::try_from(record) {
            Ok(rule) => rule,
            Err(error) => {
                tracing::error!(
                    rule = %record.id,
                    %error,
                    "the store holds a rule this build will not accept; it is not running"
                );
                continue;
            }
        };
        tracing::info!(
            rule = rule.id.as_str(),
            name = %rule.name,
            "the store holds a rule this configuration does not name; it was created \
             over the API or by an earlier configuration, and it is being installed"
        );
        registry
            .install(rule)
            .await
            .map_err(|error| StartupError::Connector(error.to_string()))?;
    }

    shared.with_broker(|broker| {
        broker.hooks_mut().install(registry.hook());
    });

    let ingest = Arc::clone(registry.ingest());
    Ok(Connectors {
        registry: Some(registry),
        ingest,
    })
}
