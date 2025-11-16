# sqlite-flux

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](https://en.cppreference.com/w/cpp/20)

**A modern C++20 type-safe, thread-safe SQLite query builder with fluent API and compile-time validation.**

## ✨ Features

- 🔒 **Thread-safe** schema caching with `std::shared_mutex`
- 🛡️ **Type-safe** queries with compile-time column validation
- 🎯 **Fluent API** for readable, chainable query construction
- ⚡ **Zero runtime overhead** with modern C++20 features
- 📦 **Easy integration** via vcpkg, CMake FetchContent
- 🔍 **Schema introspection** and automatic caching
- 🧪 **Production-ready** with comprehensive error handling

## 🚀 Quick Start
```cpp
#include <sqlite_flux/QueryBuilder.h>
#include <sqlite_flux/Analyzer.h>

sqlite_flux::Analyzer db("database.db");
db.cacheAllSchemas();  // Thread-safe schema caching

sqlite_flux::QueryFactory factory(db);

auto results = factory.FromTable("users")
    .Columns("id", "username", "email")
    .Filter("is_active", int64_t(1))
    .Filter("age", int64_t(18), sqlite_flux::CompareOp::GreaterThanOrEqual)
    .OrderBy("username")
    .Limit(10)
    .Execute();
```

## 📦 Installation

### Via vcpkg (Recommended)
```bash
vcpkg install sqlite-flux
```

### Via CMake FetchContent
```cmake
include(FetchContent)
FetchContent_Declare(
    sqlite_flux
    GIT_REPOSITORY https://github.com/YourUsername/sqlite-flux.git
    GIT_TAG v1.0.0
)
FetchContent_MakeAvailable(sqlite_flux)

target_link_libraries(your_target PRIVATE sqlite_flux::sqlite_flux)
```

## 🔧 Building from Source

**Requirements:**
- C++20 compatible compiler (MSVC 2022, GCC 11+, Clang 13+)
- CMake 3.15+
- SQLite3

**Build:**
```bash
git clone https://github.com/YourUsername/sqlite-flux.git
cd sqlite-flux
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . --config Release
```

## 📖 Documentation

See [docs/API.md](docs/API.md) for complete API reference.

### Thread Safety

- ✅ `Analyzer` class: Thread-safe for concurrent reads
- ✅ Schema cache: Uses `std::shared_mutex` for concurrent access
- ⚠️ `QueryBuilder`: Not thread-safe (create per-thread instances)

See [docs/THREAD_SAFETY.md](docs/THREAD_SAFETY.md) for details.

## 📄 License

MIT License - see [LICENSE](LICENSE) file.

## 🤝 Contributing

Contributions welcome! See [CONTRIBUTING.md](CONTRIBUTING.md).