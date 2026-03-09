/// Generate MQTT topics in a tree structure
///
/// For example with max_depth=2, topics_per_node=3, prefix="test", start_num=1:
/// - test00001
/// - test00001/01, test00001/02, test00001/03
/// - test00001/01/01, test00001/01/02, ..., test00001/03/03

pub struct TopicGenerator {
    prefix: String,
    base_topic_index: usize,
    topics_per_node: usize,
    max_depth: usize,
}

impl TopicGenerator {
    pub fn new(
        prefix: String,
        base_topic_index: usize,
        topics_per_node: usize,
        max_depth: usize,
    ) -> Self {
        TopicGenerator {
            prefix,
            base_topic_index,
            topics_per_node,
            max_depth,
        }
    }

    pub fn generate_all(&self) -> Vec<String> {
        let mut topics = Vec::new();
        let base_topic = format!("{}{:05}", self.prefix, self.base_topic_index);

        // Add root topic
        topics.push(base_topic.clone());

        // Generate topics for each depth level
        for depth in 1..=self.max_depth {
            self.generate_at_depth(&mut topics, &base_topic, depth, Vec::new());
        }

        topics
    }

    #[allow(dead_code)]
    pub fn generate_leaves_only(&self) -> Vec<String> {
        let mut topics = Vec::new();
        let base_topic = format!("{}{:05}", self.prefix, self.base_topic_index);

        // Only generate topics at max_depth (leaf nodes)
        if self.max_depth > 0 {
            self.generate_at_depth(&mut topics, &base_topic, self.max_depth, Vec::new());
        }

        topics
    }

    #[allow(dead_code)]
    pub fn generate_wildcard_subscriptions(&self) -> Vec<String> {
        let mut topics = Vec::new();
        let base_topic = format!("{}{:05}", self.prefix, self.base_topic_index);

        // Generate topics at max_depth - 1 level with wildcard
        if self.max_depth > 1 {
            self.generate_at_depth(&mut topics, &base_topic, self.max_depth - 1, Vec::new());
            // Add wildcard suffix to each topic
            topics = topics.into_iter().map(|t| format!("{}/#", t)).collect();
        } else if self.max_depth == 1 {
            // If max_depth is 1, just subscribe to base_topic/#
            topics.push(format!("{}/#", base_topic));
        }

        topics
    }

    #[allow(dead_code)]
    pub fn generate_single_wildcard(&self) -> Vec<String> {
        let base_topic = format!("{}{:05}", self.prefix, self.base_topic_index);
        vec![format!("{}/#", base_topic)]
    }

    fn generate_at_depth(
        &self,
        topics: &mut Vec<String>,
        base: &str,
        target_depth: usize,
        path: Vec<usize>,
    ) {
        if path.len() == target_depth {
            // Build the full topic path
            let mut full_path = base.to_string();
            for component in &path {
                full_path.push_str(&format!("/{:05}", component));
            }
            topics.push(full_path);
            return;
        }

        // Recursively generate for each possible child
        for i in 1..=self.topics_per_node {
            let mut new_path = path.clone();
            new_path.push(i);
            self.generate_at_depth(topics, base, target_depth, new_path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_generation() {
        let gen = TopicGenerator::new("test".to_string(), 1, 2, 2);
        let topics = gen.generate_all();

        assert!(topics.contains(&"test00001".to_string()));
        assert!(topics.contains(&"test00001/00001".to_string()));
        assert!(topics.contains(&"test00001/00002".to_string()));
        assert!(topics.contains(&"test00001/00001/00001".to_string()));
        assert!(topics.contains(&"test00001/00001/00002".to_string()));
        assert!(topics.contains(&"test00001/00002/00001".to_string()));
        assert!(topics.contains(&"test00001/00002/00002".to_string()));
    }

    #[test]
    fn test_topic_generation_leaves_only() {
        let gen = TopicGenerator::new("test".to_string(), 1, 2, 2);
        let topics = gen.generate_leaves_only();

        // Should only contain leaf topics (at max_depth)
        assert!(!topics.contains(&"test00001".to_string()));
        assert!(!topics.contains(&"test00001/00001".to_string()));
        assert!(!topics.contains(&"test00001/00002".to_string()));
        assert!(topics.contains(&"test00001/00001/00001".to_string()));
        assert!(topics.contains(&"test00001/00001/00002".to_string()));
        assert!(topics.contains(&"test00001/00002/00001".to_string()));
        assert!(topics.contains(&"test00001/00002/00002".to_string()));
        assert_eq!(topics.len(), 4); // Should have exactly 4 leaf topics (2^2)
    }

    #[test]
    fn test_wildcard_subscriptions() {
        let gen = TopicGenerator::new("test".to_string(), 1, 2, 2);
        let topics = gen.generate_wildcard_subscriptions();

        // Should contain parent topics with wildcard
        assert!(topics.contains(&"test00001/00001/#".to_string()));
        assert!(topics.contains(&"test00001/00002/#".to_string()));
        assert_eq!(topics.len(), 2); // Should have 2 parent topics (topics_per_node=2)
    }

    #[test]
    fn test_single_wildcard() {
        let gen = TopicGenerator::new("test".to_string(), 1, 2, 2);
        let topics = gen.generate_single_wildcard();

        // Should contain only one wildcard at base level
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0], "test00001/#");
    }
}
