//! A deliberately naive matcher, used as the oracle for the fast paths.

/// Whether `filter` matches `topic`.
///
/// This is the oracle. It is written for obviousness rather than speed, and
/// nothing on a hot path calls it. Both arguments are assumed already
/// validated by `mqtt_codec::topic`.
///
/// The one rule that is easy to miss: a filter whose first level is a
/// wildcard must not match a topic whose first level begins with a dollar
/// sign, so that a subscription to everything does not silently receive the
/// broker's own status topics.
pub fn matches(filter: &str, topic: &str) -> bool {
    let first_level_is_wildcard = matches!(filter.split('/').next(), Some("+") | Some("#"));
    if first_level_is_wildcard && topic.starts_with('$') {
        return false;
    }

    let filter_levels: Vec<&str> = filter.split('/').collect();
    let topic_levels: Vec<&str> = topic.split('/').collect();
    walk(&filter_levels, &topic_levels)
}

fn walk(filter: &[&str], topic: &[&str]) -> bool {
    match (filter.first(), topic.first()) {
        // A multi-level wildcard absorbs everything that remains, including
        // nothing at all, which is why `a/#` matches `a`.
        (Some(&"#"), _) => true,
        (Some(&level), Some(&actual)) => {
            (level == "+" || level == actual) && walk(&filter[1..], &topic[1..])
        }
        (None, None) => true,
        (Some(_), None) | (None, Some(_)) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Cases taken from the specification's own matching examples.
    #[test]
    fn matches_the_specification_examples() {
        assert!(matches("sport/tennis/player1/#", "sport/tennis/player1"));
        assert!(matches(
            "sport/tennis/player1/#",
            "sport/tennis/player1/ranking"
        ));
        assert!(matches(
            "sport/tennis/player1/#",
            "sport/tennis/player1/score/wimbledon"
        ));
        assert!(matches("sport/#", "sport"));
        assert!(matches("sport/tennis/+", "sport/tennis/player1"));
        assert!(matches("sport/tennis/+", "sport/tennis/player2"));
        assert!(!matches("sport/tennis/+", "sport/tennis/player1/ranking"));
        assert!(matches("sport/+", "sport/"));
        assert!(!matches("sport/+", "sport"));
        assert!(matches("+/+", "/finance"));
        assert!(matches("/+", "/finance"));
        assert!(!matches("+", "/finance"));
    }

    #[test]
    fn a_leading_wildcard_never_matches_a_dollar_topic() {
        assert!(!matches("#", "$SYS/broker/uptime"));
        assert!(!matches("+/broker/uptime", "$SYS/broker/uptime"));
        assert!(matches("$SYS/#", "$SYS/broker/uptime"));
        assert!(matches("$SYS/broker/+", "$SYS/broker/uptime"));
    }

    #[test]
    fn a_dollar_filter_does_not_match_an_ordinary_topic() {
        assert!(!matches("$SYS/#", "sport/tennis"));
    }

    #[test]
    fn empty_levels_are_ordinary_levels() {
        assert!(matches("a/+/b", "a//b"));
        assert!(matches("a//b", "a//b"));
        assert!(!matches("a/b", "a//b"));
    }

    #[test]
    fn a_multi_level_wildcard_matches_the_parent_level() {
        assert!(matches("a/#", "a"));
        assert!(matches("#", "a"));
        assert!(matches("#", "a/b/c"));
    }

    #[test]
    fn an_exact_filter_matches_only_itself() {
        assert!(matches("a/b/c", "a/b/c"));
        assert!(!matches("a/b/c", "a/b"));
        assert!(!matches("a/b", "a/b/c"));
    }
}
