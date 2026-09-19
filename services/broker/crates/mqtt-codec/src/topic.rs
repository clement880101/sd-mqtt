//! Validation of publish topic names and subscription filters.

use crate::error::CodecError;
use crate::header::Limits;

/// The prefix marking a shared subscription.
pub const SHARE_PREFIX: &str = "$share/";

fn level_count(topic: &str) -> usize {
    topic.split('/').count()
}

/// Validate a topic name as used in PUBLISH.
///
/// A topic name must be non-empty and must contain no wildcard character.
pub fn validate_name(name: &str, limits: &Limits) -> Result<(), CodecError> {
    if name.is_empty() {
        return Err(CodecError::InvalidTopicName);
    }
    if name.contains('+') || name.contains('#') {
        return Err(CodecError::InvalidTopicName);
    }
    if name.contains('\0') {
        return Err(CodecError::InvalidTopicName);
    }
    if level_count(name) > usize::from(limits.max_topic_depth) {
        return Err(CodecError::TopicNameTooDeep);
    }
    Ok(())
}

/// Validate a subscription filter.
///
/// A single-level wildcard must occupy an entire level. A multi-level
/// wildcard must occupy an entire level and be the final one.
pub fn validate_filter(filter: &str, limits: &Limits) -> Result<(), CodecError> {
    if filter.is_empty() {
        return Err(CodecError::InvalidTopicFilter);
    }
    if filter.contains('\0') {
        return Err(CodecError::InvalidTopicFilter);
    }

    let levels: Vec<&str> = filter.split('/').collect();
    if levels.len() > usize::from(limits.max_topic_depth) {
        return Err(CodecError::TopicFilterTooDeep);
    }

    let last = levels.len() - 1;
    for (index, level) in levels.iter().enumerate() {
        let has_plus = level.contains('+');
        let has_hash = level.contains('#');

        if has_plus && *level != "+" {
            return Err(CodecError::InvalidTopicFilter);
        }
        if has_hash {
            if *level != "#" {
                return Err(CodecError::InvalidTopicFilter);
            }
            if index != last {
                return Err(CodecError::InvalidTopicFilter);
            }
        }
    }

    Ok(())
}

/// Split a shared subscription filter into its group and inner filter.
///
/// Returns `Ok(None)` for an ordinary filter. The group must be non-empty
/// and must contain no wildcard or separator, and an inner filter must
/// follow it.
pub fn parse_shared(filter: &str) -> Result<Option<(&str, &str)>, CodecError> {
    let Some(rest) = filter.strip_prefix(SHARE_PREFIX) else {
        return Ok(None);
    };

    let Some((group, inner)) = rest.split_once('/') else {
        return Err(CodecError::InvalidTopicFilter);
    };

    if group.is_empty() || group.contains('+') || group.contains('#') {
        return Err(CodecError::InvalidTopicFilter);
    }
    if inner.is_empty() {
        return Err(CodecError::InvalidTopicFilter);
    }

    Ok(Some((group, inner)))
}

/// Validate a filter as it appears in SUBSCRIBE or UNSUBSCRIBE.
///
/// Returns the share group and inner filter for a shared subscription,
/// or `None` for an ordinary one. This is the correct entry point for
/// subscription filters: calling [`parse_shared`] or [`validate_filter`]
/// alone accepts filters the other would reject.
pub fn validate_subscription<'a>(
    filter: &'a str,
    limits: &Limits,
) -> Result<Option<(&'a str, &'a str)>, CodecError> {
    let shared = parse_shared(filter)?;
    let inner = shared.map(|(_, inner)| inner).unwrap_or(filter);
    validate_filter(inner, limits)?;
    Ok(shared)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CodecError;

    fn depth(n: u16) -> Limits {
        Limits {
            max_topic_depth: n,
            ..Limits::default()
        }
    }

    #[test]
    fn accepts_ordinary_topic_names() {
        for name in ["a", "a/b/c", "/", "sport/tennis/player1", " "] {
            assert!(validate_name(name, &depth(16)).is_ok(), "{name:?}");
        }
    }

    #[test]
    fn rejects_an_empty_topic_name() {
        assert_eq!(
            validate_name("", &depth(16)).unwrap_err(),
            CodecError::InvalidTopicName
        );
    }

    #[test]
    fn rejects_wildcards_in_a_topic_name() {
        for name in ["a/+", "a/#", "+", "#", "a/+/b"] {
            assert_eq!(
                validate_name(name, &depth(16)).unwrap_err(),
                CodecError::InvalidTopicName,
                "{name:?}"
            );
        }
    }

    #[test]
    fn rejects_a_topic_name_deeper_than_the_limit() {
        assert!(validate_name("a/b/c", &depth(3)).is_ok());
        assert_eq!(
            validate_name("a/b/c/d", &depth(3)).unwrap_err(),
            CodecError::TopicNameTooDeep
        );
    }

    #[test]
    fn accepts_well_formed_filters() {
        for filter in ["a", "a/b", "+", "#", "a/#", "a/+/b", "+/+/#", "/+"] {
            assert!(validate_filter(filter, &depth(16)).is_ok(), "{filter:?}");
        }
    }

    #[test]
    fn rejects_a_multi_level_wildcard_that_is_not_last() {
        for filter in ["#/a", "a/#/b"] {
            assert_eq!(
                validate_filter(filter, &depth(16)).unwrap_err(),
                CodecError::InvalidTopicFilter,
                "{filter:?}"
            );
        }
    }

    #[test]
    fn rejects_a_wildcard_sharing_a_level_with_other_characters() {
        for filter in ["a+", "+a", "a#", "#a", "sport+"] {
            assert_eq!(
                validate_filter(filter, &depth(16)).unwrap_err(),
                CodecError::InvalidTopicFilter,
                "{filter:?}"
            );
        }
    }

    #[test]
    fn rejects_an_empty_filter() {
        assert_eq!(
            validate_filter("", &depth(16)).unwrap_err(),
            CodecError::InvalidTopicFilter
        );
    }

    #[test]
    fn recognises_a_shared_subscription_and_splits_it() {
        let parsed = parse_shared("$share/group-a/sport/#").expect("valid shared filter");
        assert_eq!(parsed, Some(("group-a", "sport/#")));
        assert_eq!(parse_shared("sport/#").unwrap(), None);
    }

    #[test]
    fn rejects_a_shared_subscription_with_a_bad_group() {
        // Empty group, and a group containing a wildcard.
        assert!(parse_shared("$share//sport").is_err());
        assert!(parse_shared("$share/gr+up/sport").is_err());
        // Missing the filter after the group.
        assert!(parse_shared("$share/group").is_err());
        assert!(parse_shared("$share/group/").is_err());
    }

    #[test]
    fn rejects_a_null_character_in_a_topic_name() {
        assert_eq!(
            validate_name("a/\0/b", &depth(16)).unwrap_err(),
            CodecError::InvalidTopicName
        );
    }

    #[test]
    fn rejects_a_null_character_in_a_topic_filter() {
        assert_eq!(
            validate_filter("a/\0/b", &depth(16)).unwrap_err(),
            CodecError::InvalidTopicFilter
        );
    }

    #[test]
    fn validate_subscription_accepts_an_ordinary_filter() {
        assert_eq!(validate_subscription("sport/#", &depth(16)), Ok(None));
    }

    #[test]
    fn validate_subscription_splits_and_validates_a_shared_filter() {
        assert_eq!(
            validate_subscription("$share/g/a/#", &depth(16)),
            Ok(Some(("g", "a/#")))
        );
    }

    #[test]
    fn validate_subscription_rejects_an_empty_share_group() {
        assert!(validate_subscription("$share//x", &depth(16)).is_err());
    }

    #[test]
    fn validate_subscription_rejects_a_misplaced_wildcard_inside_a_shared_filter() {
        assert!(validate_subscription("$share/g/a/#/b", &depth(16)).is_err());
    }

    #[test]
    fn validate_subscription_rejects_a_misplaced_wildcard_in_an_ordinary_filter() {
        assert!(validate_subscription("a/#/b", &depth(16)).is_err());
    }
}
