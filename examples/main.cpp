// examples/main.cpp
#include "ConnectionPool.h"
#include "QueryBuilder.h"
#include "InsertBuilder.h"
#include "UpdateBuilder.h"
#include "DeleteBuilder.h"
#include "Analyzer.h"
#include "ValueVisitor.h"
#include <iostream>
#include <filesystem>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>

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

// Test function for thread-safe SELECT operations
void testThreadSafeQuery(int threadId, sqlite_flux::ConnectionPool& pool)
{
	try
	{
		std::cout << "[SELECT Thread " << threadId << "] Starting...\n";

		auto conn = pool.acquire();

		sqlite_flux::QueryFactory factory(*conn);
		auto results = factory.FromTable("users")
			.Columns("id", "username")
			.Filter("is_active", int64_t(1))
			.Limit(3)
			.Execute();

		std::cout << "[SELECT Thread " << threadId << "] Found " << results.size() << " users\n";
	}
	catch (const std::exception& e)
	{
		std::cerr << "[SELECT Thread " << threadId << "] Error: " << e.what() << "\n";
	}
} // end of testThreadSafeQuery

// Test function for thread-safe INSERT operations
void testThreadSafeInsert(int threadId, sqlite_flux::ConnectionPool& pool, std::atomic<int64_t>& totalInserted)
{
	try
	{
		std::cout << "[INSERT Thread " << threadId << "] Starting...\n";

		auto conn = pool.acquire();
		sqlite_flux::QueryFactory factory(*conn);

		// Insert a new user
		auto userId = factory.InsertInto("users")
			.Values({
				{"username", std::string("thread_user_") + std::to_string(threadId)},
				{"email", std::string("thread") + std::to_string(threadId) + std::string("@example.com")},
				{"is_active", int64_t(1)},
				{"created_at", int64_t(std::time(nullptr))}
				})
			.Execute();

		std::cout << "[INSERT Thread " << threadId << "] Inserted user ID: " << userId << "\n";
		totalInserted.fetch_add(1, std::memory_order_relaxed);
	}
	catch (const std::exception& e)
	{
		std::cerr << "[INSERT Thread " << threadId << "] Error: " << e.what() << "\n";
	}
} // end of testThreadSafeInsert

// Test function for thread-safe UPDATE operations
void testThreadSafeUpdate(int threadId, sqlite_flux::ConnectionPool& pool, std::atomic<int64_t>& totalUpdated)
{
	try
	{
		std::cout << "[UPDATE Thread " << threadId << "] Starting...\n";

		auto conn = pool.acquire();
		sqlite_flux::QueryFactory factory(*conn);

		// Update users created by this thread
		auto rowsUpdated = factory.UpdateTable("users")
			.Set("is_active", int64_t(0))
			.Where("username", std::string("thread_user_") + std::to_string(threadId))
			.Execute();

		std::cout << "[UPDATE Thread " << threadId << "] Updated " << rowsUpdated << " rows\n";
		totalUpdated.fetch_add(rowsUpdated, std::memory_order_relaxed);
	}
	catch (const std::exception& e)
	{
		std::cerr << "[UPDATE Thread " << threadId << "] Error: " << e.what() << "\n";
	}
} // end of testThreadSafeUpdate

// Test function for thread-safe DELETE operations
void testThreadSafeDelete(int threadId, sqlite_flux::ConnectionPool& pool, std::atomic<int64_t>& totalDeleted)
{
	try
	{
		std::cout << "[DELETE Thread " << threadId << "] Starting...\n";

		auto conn = pool.acquire();
		sqlite_flux::QueryFactory factory(*conn);

		// Delete inactive users
		auto rowsDeleted = factory.DeleteFrom("users")
			.Where("username", std::string("thread_user_") + std::to_string(threadId))
			.Where("is_active", int64_t(0))
			.Execute();

		std::cout << "[DELETE Thread " << threadId << "] Deleted " << rowsDeleted << " rows\n";
		totalDeleted.fetch_add(rowsDeleted, std::memory_order_relaxed);
	}
	catch (const std::exception& e)
	{
		std::cerr << "[DELETE Thread " << threadId << "] Error: " << e.what() << "\n";
	}
} // end of testThreadSafeDelete

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
		// Test 2: Connection Pool - SELECT (Multi-threaded)
		// ====================================================================
		std::cout << "=== Test 2: Connection Pool - SELECT Operations ===\n";
		{
			sqlite_flux::ConnectionPool pool(dbPath, 3, true);

			std::cout << "Connection pool created:\n";
			std::cout << "  Total connections: " << pool.size() << "\n";
			std::cout << "  Available: " << pool.available() << "\n\n";

			std::cout << "Testing 5 concurrent SELECT threads with 3 connections:\n";
			std::vector<std::thread> threads;

			auto startTime = std::chrono::steady_clock::now();

			for (int i = 0; i < 5; ++i)
			{
				threads.emplace_back(testThreadSafeQuery, i + 1, std::ref(pool));
			} // end of for

			for (auto& thread : threads)
			{
				thread.join();
			} // end of for

			auto endTime = std::chrono::steady_clock::now();
			auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

			std::cout << "\nAll SELECT threads completed in " << duration.count() << "ms\n";
			std::cout << "Pool stats - Available: " << pool.available()
				<< ", In use: " << pool.inUse() << "\n\n";
		} // end of scope

		// ====================================================================
		// Test 3: Connection Pool - INSERT (Multi-threaded)
		// ====================================================================
		std::cout << "=== Test 3: Connection Pool - INSERT Operations ===\n";
		{
			sqlite_flux::ConnectionPool pool(dbPath, 3, true);
			std::atomic<int64_t> totalInserted{ 0 };

			std::cout << "Testing 10 concurrent INSERT threads:\n";
			std::vector<std::thread> threads;

			auto startTime = std::chrono::steady_clock::now();

			for (int i = 0; i < 10; ++i)
			{
				threads.emplace_back(testThreadSafeInsert, i + 1, std::ref(pool), std::ref(totalInserted));
			} // end of for

			for (auto& thread : threads)
			{
				thread.join();
			} // end of for

			auto endTime = std::chrono::steady_clock::now();
			auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

			std::cout << "\nAll INSERT threads completed in " << duration.count() << "ms\n";
			std::cout << "Total rows inserted: " << totalInserted.load() << "\n";
			std::cout << "Throughput: " << (totalInserted.load() * 1000 / duration.count()) << " inserts/sec\n\n";
		} // end of scope

		// ====================================================================
		// Test 4: Connection Pool - UPDATE (Multi-threaded)
		// ====================================================================
		std::cout << "=== Test 4: Connection Pool - UPDATE Operations ===\n";
		{
			sqlite_flux::ConnectionPool pool(dbPath, 3, true);
			std::atomic<int64_t> totalUpdated{ 0 };

			std::cout << "Testing 10 concurrent UPDATE threads:\n";
			std::vector<std::thread> threads;

			auto startTime = std::chrono::steady_clock::now();

			for (int i = 0; i < 10; ++i)
			{
				threads.emplace_back(testThreadSafeUpdate, i + 1, std::ref(pool), std::ref(totalUpdated));
			} // end of for

			for (auto& thread : threads)
			{
				thread.join();
			} // end of for

			auto endTime = std::chrono::steady_clock::now();
			auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

			std::cout << "\nAll UPDATE threads completed in " << duration.count() << "ms\n";
			std::cout << "Total rows updated: " << totalUpdated.load() << "\n";
			std::cout << "Throughput: " << (totalUpdated.load() * 1000 / duration.count()) << " updates/sec\n\n";
		} // end of scope

		// ====================================================================
		// Test 5: Connection Pool - DELETE (Multi-threaded)
		// ====================================================================
		std::cout << "=== Test 5: Connection Pool - DELETE Operations ===\n";
		{
			sqlite_flux::ConnectionPool pool(dbPath, 3, true);
			std::atomic<int64_t> totalDeleted{ 0 };

			std::cout << "Testing 10 concurrent DELETE threads:\n";
			std::vector<std::thread> threads;

			auto startTime = std::chrono::steady_clock::now();

			for (int i = 0; i < 10; ++i)
			{
				threads.emplace_back(testThreadSafeDelete, i + 1, std::ref(pool), std::ref(totalDeleted));
			} // end of for

			for (auto& thread : threads)
			{
				thread.join();
			} // end of for

			auto endTime = std::chrono::steady_clock::now();
			auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

			std::cout << "\nAll DELETE threads completed in " << duration.count() << "ms\n";
			std::cout << "Total rows deleted: " << totalDeleted.load() << "\n";
			std::cout << "Throughput: " << (totalDeleted.load() * 1000 / duration.count()) << " deletes/sec\n\n";
		} // end of scope

		// ====================================================================
		// Test 6: Thread-Safe Schema Cache
		// ====================================================================
		std::cout << "=== Test 6: Thread-Safe Schema Cache ===\n";
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
		// Test 7: Mixed Workload (Realistic Scenario)
		// ====================================================================
		std::cout << "=== Test 7: Mixed Workload (SELECT/INSERT/UPDATE) ===\n";
		{
			sqlite_flux::ConnectionPool pool(dbPath, 5, true);
			std::atomic<int64_t> totalInserted{ 0 };
			std::atomic<int64_t> totalUpdated{ 0 };

			std::cout << "Testing 20 mixed operations (50% SELECT, 30% INSERT, 20% UPDATE):\n";
			std::vector<std::thread> threads;

			auto startTime = std::chrono::steady_clock::now();

			for (int i = 0; i < 20; ++i)
			{
				if (i % 10 < 5)  // 50% SELECT
				{
					threads.emplace_back(testThreadSafeQuery, i + 1, std::ref(pool));
				}
				else if (i % 10 < 8)  // 30% INSERT
				{
					threads.emplace_back(testThreadSafeInsert, i + 100, std::ref(pool), std::ref(totalInserted));
				}
				else  // 20% UPDATE
				{
					threads.emplace_back(testThreadSafeUpdate, (i % 10) + 100, std::ref(pool), std::ref(totalUpdated));
				} // end of if-else
			} // end of for

			for (auto& thread : threads)
			{
				thread.join();
			} // end of for

			auto endTime = std::chrono::steady_clock::now();
			auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

			std::cout << "\nAll mixed operations completed in " << duration.count() << "ms\n";
			std::cout << "  Inserts: " << totalInserted.load() << "\n";
			std::cout << "  Updates: " << totalUpdated.load() << "\n";
			std::cout << "  Throughput: " << (20 * 1000 / duration.count()) << " ops/sec\n\n";
		} // end of scope

		// ====================================================================
		// Test 8: Database Statistics
		// ====================================================================
		std::cout << "=== Test 8: Database Statistics ===\n";
		{
			sqlite_flux::Analyzer db(dbPath);

			sqlite_flux::QueryFactory factory(db);

			int64_t totalUsers = factory.FromTable("users").Count();
			std::cout << "Total users: " << totalUsers << "\n";

			int64_t activeUsers = factory.FromTable("users")
				.Filter("is_active", int64_t(1))
				.Count();
			std::cout << "Active users: " << activeUsers << "\n";

			int64_t totalCategories = factory.FromTable("categories").Count();
			std::cout << "Total categories: " << totalCategories << "\n\n";
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