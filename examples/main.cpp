// examples/main.cpp
#include "ConnectionPool.h"
#include "QueryBuilder.h"
#include "Analyzer.h"
#include "ValueVisitor.h"
#include <iostream>
#include <filesystem>
#include <vector>
#include <thread>
#include <chrono>

namespace fs = std::filesystem;

// Function to find database file
std::string findDatabase(const std::string& filename)
{
	// Possible paths to check (relative to build/bin/Debug or build/bin/Release)
	std::vector<std::string> searchPaths = {
		filename,
		"databases/" + filename,
		"../databases/" + filename,
		"../../databases/" + filename,
		"../../../databases/" + filename,
		"../../../../databases/" + filename
	};

	std::cout << "Current working directory: " << fs::current_path() << "\n";
	std::cout << "Searching for database: " << filename << "\n\n";

	for (const auto& path : searchPaths)
	{
		std::cout << "  Checking: " << path << " ... ";
		if (fs::exists(path))
		{
			std::cout << "FOUND!\n";
			return fs::absolute(path).string();
		} // end of if
		std::cout << "not found\n";
	} // end of for

	return "";
} // end of findDatabase

// Test function for thread-safe operations
void testThreadSafeQuery(int threadId, sqlite_flux::ConnectionPool& pool)
{
	try
	{
		std::cout << "[Thread " << threadId << "] Starting...\n";

		// Acquire connection from pool
		auto conn = pool.acquire();

		std::cout << "[Thread " << threadId << "] Got connection\n";

		// Execute query
		sqlite_flux::QueryFactory factory(*conn);
		auto results = factory.FromTable("users")
			.Columns("id", "username")
			.Filter("is_active", int64_t(1))
			.Limit(3)
			.Execute();

		std::cout << "[Thread " << threadId << "] Found " << results.size() << " users\n";

		// Simulate work
		std::this_thread::sleep_for(std::chrono::milliseconds(100));

		std::cout << "[Thread " << threadId << "] Completed\n";

	} // end of try
	catch (const std::exception& e)
	{
		std::cerr << "[Thread " << threadId << "] Error: " << e.what() << "\n";
	} // end of catch
} // end of testThreadSafeQuery

int main()
{
	try
	{
		// Find the database
		std::string dbPath = findDatabase("testdb.db");

		if (dbPath.empty())
		{
			std::cerr << "\nERROR: Could not find testdb.db\n";
			std::cerr << "Please run create_testdb.ps1 first.\n";
			return 1;
		} // end of if

		std::cout << "\nUsing database: " << dbPath << "\n\n";

		// ====================================================================
		// Test 1: Basic Analyzer (Single-threaded)
		// ====================================================================
		std::cout << "=== Test 1: Basic Analyzer ===\n";
		{
			sqlite_flux::Analyzer db(dbPath);

			if (!db.isOpen())
			{
				std::cerr << "Failed to open database: " << db.getLastError() << "\n";
				return 1;
			} // end of if

			std::cout << "Database opened successfully\n";
			std::cout << "WAL mode enabled: " << (db.isWALMode() ? "Yes" : "No") << "\n";

			// Cache all schemas
			db.cacheAllSchemas();
			std::cout << "Schemas cached: " << (db.isSchemaCached() ? "Yes" : "No") << "\n";

			// Create factory and test query
			sqlite_flux::QueryFactory factory(db);

			auto users = factory.FromTable("users")
				.Columns("id", "username", "email")
				.Filter("is_active", int64_t(1))
				.Limit(5)
				.Execute();

			std::cout << "Found " << users.size() << " active users:\n";
			for (const auto& row : users)
			{
				auto id = sqlite_flux::getValue<int64_t>(row, "id");
				auto username = sqlite_flux::getValue<std::string>(row, "username");

				if (id && username)
				{
					std::cout << "  " << *id << ". " << *username << "\n";
				} // end of if
			} // end of for

			std::cout << "\n";
		} // end of scope

		// ====================================================================
		// Test 2: Connection Pool (Multi-threaded)
		// ====================================================================
		std::cout << "=== Test 2: Connection Pool ===\n";
		{
			// Create connection pool with 3 connections
			sqlite_flux::ConnectionPool pool(dbPath, 3, true);

			std::cout << "Connection pool created:\n";
			std::cout << "  Total connections: " << pool.size() << "\n";
			std::cout << "  Available: " << pool.available() << "\n\n";

			// Test simple acquire/release
			std::cout << "Testing acquire/release:\n";
			{
				auto conn1 = pool.acquire();
				std::cout << "  Acquired connection 1, available: " << pool.available() << "\n";

				{
					auto conn2 = pool.acquire();
					std::cout << "  Acquired connection 2, available: " << pool.available() << "\n";
				} // conn2 auto-released here

				std::cout << "  Released connection 2, available: " << pool.available() << "\n";
			} // conn1 auto-released here

			std::cout << "  Released connection 1, available: " << pool.available() << "\n\n";

			// Test multi-threaded access
			std::cout << "Testing 5 concurrent threads with 3 connections:\n";
			std::vector<std::thread> threads;

			auto startTime = std::chrono::steady_clock::now();

			for (int i = 0; i < 5; ++i)
			{
				threads.emplace_back(testThreadSafeQuery, i + 1, std::ref(pool));
			} // end of for

			// Wait for all threads
			for (auto& thread : threads)
			{
				thread.join();
			} // end of for

			auto endTime = std::chrono::steady_clock::now();
			auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

			std::cout << "\nAll threads completed in " << duration.count() << "ms\n";
			std::cout << "Pool stats - Available: " << pool.available()
				<< ", In use: " << pool.inUse() << "\n\n";
		} // end of scope

		// ====================================================================
		// Test 3: Schema Cache (Thread-safe)
		// ====================================================================
		std::cout << "=== Test 3: Thread-Safe Schema Cache ===\n";
		{
			sqlite_flux::Analyzer db(dbPath);
			db.cacheAllSchemas();

			std::cout << "Schema cache initialized\n";

			// Multiple threads reading schema concurrently
			std::vector<std::thread> threads;

			for (int i = 0; i < 5; ++i)
			{
				threads.emplace_back([&db, i]() {
					auto schema = db.getTableSchema("users");
					std::cout << "[Thread " << i << "] Retrieved schema with "
						<< schema.size() << " columns\n";
					});
			} // end of for

			for (auto& thread : threads)
			{
				thread.join();
			} // end of for

			std::cout << "\n";
		} // end of scope

		// ====================================================================
		// Test 4: Database Statistics
		// ====================================================================
		std::cout << "=== Test 4: Database Statistics ===\n";
		{
			sqlite_flux::Analyzer db(dbPath);
			db.cacheAllSchemas();

			sqlite_flux::QueryFactory factory(db);

			int64_t totalUsers = factory.FromTable("users").Count();
			std::cout << "Total users: " << totalUsers << "\n";

			int64_t activeUsers = factory.FromTable("users")
				.Filter("is_active", int64_t(1))
				.Count();
			std::cout << "Active users: " << activeUsers << "\n";

			int64_t totalCategories = factory.FromTable("categories").Count();
			std::cout << "Total categories: " << totalCategories << "\n";

			int64_t rootCategories = factory.FromTable("categories")
				.Filter("level", int64_t(0))
				.Count();
			std::cout << "Root categories: " << rootCategories << "\n\n";
		} // end of scope

		std::cout << "=== All tests completed successfully! ===\n";

		return 0;
	} // end of try
	catch (const std::exception& e)
	{
		std::cerr << "Fatal error: " << e.what() << "\n";
		return 1;
	} // end of catch
} // end of main