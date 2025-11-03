pub fn like_match(value: &str, pattern: &str, case_sensitive: bool) -> bool {
    let value_to_match = if case_sensitive {
        value.to_string()
    } else {
        value.to_lowercase()
    };
    let pattern_to_match = if case_sensitive {
        pattern.to_string()
    } else {
        pattern.to_lowercase()
    };

    like_match_recursive(&value_to_match, &pattern_to_match)
}

fn like_match_recursive(value: &str, pattern: &str) -> bool {
    let mut val_chars = value.chars();
    let mut pat_chars = pattern.chars().peekable();

    loop {
        match pat_chars.next() {
            Some('%') => {
                if pat_chars.peek().is_none() {
                    return true;
                }
                let remaining_pattern: String = pat_chars.collect();
                for i in 0..=value.len() {
                    if like_match_recursive(&value[i..], &remaining_pattern) {
                        return true;
                    }
                }
                return false;
            }
            Some('_') => {
                if val_chars.next().is_none() {
                    return false;
                }
            }
            Some(p) => {
                if let Some(v) = val_chars.next() {
                    if v != p {
                        return false;
                    }
                } else {
                    return false;
                }
            }
            None => return val_chars.next().is_none(),
        }
    }
}

pub fn regex_match(value: &str, pattern: &str) -> bool {
    // Simplified regex implementation - just handles anchors
    // A full regex engine would require an external crate like `regex`

    let starts_with_anchor = pattern.starts_with('^');
    let ends_with_anchor = pattern.ends_with('$');

    let clean_pattern = pattern.trim_start_matches('^').trim_end_matches('$');

    if starts_with_anchor && ends_with_anchor {
        // Exact match needed
        value == clean_pattern
    } else if starts_with_anchor {
        // Must start with pattern
        value.starts_with(clean_pattern)
    } else if ends_with_anchor {
        // Must end with pattern
        value.ends_with(clean_pattern)
    } else {
        // Pattern can appear anywhere
        value.contains(clean_pattern)
    }
}
