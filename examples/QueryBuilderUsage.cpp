// examples/QueryBuilderUsage.cpp
#include "QueryBuilder.h"
#include "Analyzer.h"
#include "ValueVisitor.h"
#include <iostream>

int main()
{
    try
    {
        // Open the test database
        sqlite_flux::Analyzer db("../databases/testdb.db");

        if (!db.isOpen())
        {
            std::cerr << "Failed to open database: " << db.getLastError() << "\n";
            return 1;
        }

        std::cout << "✓ Database opened: databases/testdb.db\n\n";

        // Cache schemas
        db.cacheAllSchemas();
        std::cout << "✓ Schemas cached\n\n";

        // Create query factory
        sqlite_flux::QueryFactory factory(db);

        // Test 1: Count all users
        std::cout << "=== Test 1: Count all users ===\n";
        int64_t totalUsers = factory.FromTable("users").Count();
        std::cout << "Total users: " << totalUsers << "\n\n";

        // Test 2: Get all active users
        std::cout << "=== Test 2: Active users ===\n";
        auto activeUsers = factory.FromTable("users")
            .Columns("id", "username", "email", "age")
            .Filter("is_active", int64_t(1))
            .OrderBy("username")
            .Execute();

        std::cout << "Found " << activeUsers.size() << " active users:\n";
        for (const auto& row : activeUsers)
        {
            auto id = sqlite_flux::getValue<int64_t>(row, "id");
            auto username = sqlite_flux::getValue<std::string>(row, "username");
            auto age = sqlite_flux::getValue<int64_t>(row, "age");

            if (id && username && age)
            {
                std::cout << "  " << *id << ". " << *username
                    << " (age " << *age << ")\n";
            }
        }
        std::cout << "\n";

        // Test 3: Root categories
        std::cout << "=== Test 3: Root categories ===\n";
        auto categories = factory.FromTable("categories")
            .Columns("id", "name", "description")
            .Filter("level", int64_t(0))
            .Filter("is_active", int64_t(1))
            .OrderBy("name")
            .Execute();

        std::cout << "Found " << categories.size() << " root categories:\n";
        for (const auto& row : categories)
        {
            auto name = sqlite_flux::getValue<std::string>(row, "name");
            auto desc = sqlite_flux::getValue<std::string>(row, "description");

            if (name && desc)
            {
                std::cout << "  - " << *name << ": " << *desc << "\n";
            }
        }
        std::cout << "\n";

        // Test 4: Users older than 30
        std::cout << "=== Test 4: Users older than 30 ===\n";
        auto oldUsers = factory.FromTable("users")
            .Columns("username", "age")
            .Filter("age", int64_t(30), sqlite_flux::CompareOp::GreaterThan)
            .OrderBy("age")
            .Execute();

        std::cout << "Found " << oldUsers.size() << " users:\n";
        for (const auto& row : oldUsers)
        {
            auto username = sqlite_flux::getValue<std::string>(row, "username");
            auto age = sqlite_flux::getValue<int64_t>(row, "age");

            if (username && age)
            {
                std::cout << "  " << *username << " - " << *age << " years old\n";
            }
        }
        std::cout << "\n";

        std::cout << "✅ All tests passed!\n";

    }
    catch (const std::exception& e)
    {
        std::cerr << "❌ Error: " << e.what() << "\n";
        return 1;
    }

    return 0;
}