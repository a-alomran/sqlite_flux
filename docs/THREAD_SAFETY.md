# Thread Safety Guarantees

## Analyzer Class
- ✅ Thread-safe for concurrent read operations (queries)
- ✅ Schema cache uses std::shared_mutex for concurrent reads
- ⚠️ Write operations (execute, transactions) are serialized
- Recommendation: One Analyzer instance per thread for best performance

## QueryBuilder Class
- ⚠️ NOT thread-safe (by design)
- Each QueryBuilder instance should be used by a single thread
- Safe: Create separate QueryBuilder instances per thread

## Usage Examples
[Include your thread-safe examples]