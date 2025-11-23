// examples/web_example.cpp
#include "ConnectionPool.h"
#include "AsyncExecutor.h"
#include "QueryBuilder.h"
#include "InsertBuilder.h"
#include "UpdateBuilder.h"
#include "DeleteBuilder.h"
#include "ValueVisitor.h"
#include <iostream>
#include <chrono>
#include <thread>
#include <vector>
#include <atomic>

// Simulated web request handler - SELECT
void handleSelectRequest(int requestId, sqlite_flux::ConnectionPool& pool)
{
	try
	{
		std::cout << "[SELECT " << requestId << "] Starting\n";

		// Acquire connection from pool (RAII - auto-released)
		auto conn = pool.acquire();

		// Build and execute query
		sqlite_flux::QueryFactory factory(*conn);
		auto results = factory.FromTable("users")
			.Columns("id", "username", "email")
			.Filter("is_active", int64_t(1))
			.Limit(5)
			.Execute();

		std::cout << "[SELECT " << requestId << "] Found " << results.size() << " users\n";

		// Connection auto-released here when conn goes out of scope
	}
	catch (const std::exception& e)
	{
		std::cerr << "[SELECT " << requestId << "] Error: " << e.what() << "\n";
	}
} // end of handleSelectRequest

// Simulated web request handler - INSERT (user registration)
void handleInsertRequest(int requestId, sqlite_flux::ConnectionPool& pool, std::atomic<int64_t>& totalInserted)
{
	try
	{
		std::cout << "[INSERT " << requestId << "] Registering new user\n";

		auto conn = pool.acquire();
		sqlite_flux::QueryFactory factory(*conn);

		// Simulate user registration
		auto userId = factory.InsertInto("users")
			.Values({
				{"username", std::string("user_") + std::to_string(requestId)},
				{"email", std::string("user") + std::to_string(requestId) + std::string("@example.com")},
				{"is_active", int64_t(1)},
				{"created_at", int64_t(std::time(nullptr))}
				})
			.Execute();

		std::cout << "[INSERT " << requestId << "] Created user ID: " << userId << "\n";
		totalInserted.fetch_add(1, std::memory_order_relaxed);
	}
	catch (const std::exception& e)
	{
		std::cerr << "[INSERT " << requestId << "] Error: " << e.what() << "\n";
	}
} // end of handleInsertRequest

// Simulated web request handler - UPDATE (user activity)
void handleUpdateRequest(int requestId, sqlite_flux::ConnectionPool& pool, std::atomic<int64_t>& totalUpdated)
{
	try
	{
		std::cout << "[UPDATE " << requestId << "] Updating user activity\n";

		auto conn = pool.acquire();
		sqlite_flux::QueryFactory factory(*conn);

		// Simulate updating user's last login time
		auto rowsAffected = factory.UpdateTable("users")
			.Set("last_login", int64_t(std::time(nullptr)))
			.Where("username", std::string("user_") + std::to_string(requestId))
			.Execute();

		std::cout << "[UPDATE " << requestId << "] Updated " << rowsAffected << " rows\n";
		totalUpdated.fetch_add(rowsAffected, std::memory_order_relaxed);
	}
	catch (const std::exception& e)
	{
		std::cerr << "[UPDATE " << requestId << "] Error: " << e.what() << "\n";
	}
} // end of handleUpdateRequest

// Simulated web request handler - DELETE (cleanup old sessions)
void handleDeleteRequest(int requestId, sqlite_flux::ConnectionPool& pool, std::atomic<int64_t>& totalDeleted)
{
	try
	{
		std::cout << "[DELETE " << requestId << "] Cleaning up sessions\n";

		auto conn = pool.acquire();
		sqlite_flux::QueryFactory factory(*conn);

		// Simulate deleting old sessions
		auto rowsDeleted = factory.DeleteFrom("sessions")
			.Where("user_id", int64_t(requestId))
			.Where("expired", int64_t(1))
			.Execute();

		std::cout << "[DELETE " << requestId << "] Deleted " << rowsDeleted << " sessions\n";
		totalDeleted.fetch_add(rowsDeleted, std::memory_order_relaxed);
	}
	catch (const std::exception& e)
	{
		std::cerr << "[DELETE " << requestId << "] Error: " << e.what() << "\n";
	}
} // end of handleDeleteRequest

int main()
{
	try
	{
		std::cout << "=== sqlite_flux v1.1.0 Web Application Example ===\n\n";

		// Find database path
		std::string dbPath = "../../../databases/testdb.db";  // From build/bin/Release

		// Create test database and tables if needed
		{
			sqlite_flux::Analyzer setup(dbPath);

			// Create users table
			setup.execute(R"(
				CREATE TABLE IF NOT EXISTS users (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					username TEXT NOT NULL UNIQUE,
					email TEXT NOT NULL,
					is_active INTEGER DEFAULT 1,
					created_at INTEGER,
					last_login INTEGER
				)
			)");

			// Create sessions table
			setup.execute(R"(
				CREATE TABLE IF NOT EXISTS sessions (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					user_id INTEGER NOT NULL,
					expired INTEGER DEFAULT 0,
					created_at INTEGER
				)
			)");

			std::cout << "✓ Database initialized\n\n";
		} // end of setup scope

		// ====================================================================
		// Example 1: Concurrent SELECT Operations
		// ====================================================================
		std::cout << "=== Example 1: Concurrent SELECT (Read-Heavy) ===\n";
		{
			sqlite_flux::ConnectionPool pool(dbPath, 5, true);

			std::cout << "Connection pool: " << pool.size() << " connections\n";
			std::cout << "Simulating 10 concurrent SELECT requests...\n\n";

			std::vector<std::thread> threads;
			auto startTime = std::chrono::steady_clock::now();

			for (int i = 0; i < 10; ++i)
			{
				threads.emplace_back(handleSelectRequest, i + 1, std::ref(pool));
			} // end of for

			for (auto& thread : threads)
			{
				thread.join();
			} // end of for

			auto endTime = std::chrono::steady_clock::now();
			auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

			std::cout << "\n✓ Completed in " << duration.count() << "ms\n";
			std::cout << "  Pool available: " << pool.available() << "/" << pool.size() << "\n\n";
		} // end of scope

		// ====================================================================
		// Example 2: Concurrent INSERT Operations (User Registration)
		// ====================================================================
		std::cout << "=== Example 2: Concurrent INSERT (User Registration) ===\n";
		{
			sqlite_flux::ConnectionPool pool(dbPath, 5, true);
			std::atomic<int64_t> totalInserted{ 0 };

			std::cout << "Simulating 20 concurrent user registrations...\n\n";

			std::vector<std::thread> threads;
			auto startTime = std::chrono::steady_clock::now();

			for (int i = 0; i < 20; ++i)
			{
				threads.emplace_back(handleInsertRequest, i + 1, std::ref(pool), std::ref(totalInserted));
			} // end of for

			for (auto& thread : threads)
			{
				thread.join();
			} // end of for

			auto endTime = std::chrono::steady_clock::now();
			auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

			std::cout << "\n✓ Completed in " << duration.count() << "ms\n";
			std::cout << "  Total users inserted: " << totalInserted.load() << "\n";
			std::cout << "  Throughput: " << (totalInserted.load() * 1000 / duration.count()) << " inserts/sec\n\n";
		} // end of scope

		// ====================================================================
		// Example 3: Concurrent UPDATE Operations (User Activity)
		// ====================================================================
		std::cout << "=== Example 3: Concurrent UPDATE (User Activity) ===\n";
		{
			sqlite_flux::ConnectionPool pool(dbPath, 5, true);
			std::atomic<int64_t> totalUpdated{ 0 };

			std::cout << "Simulating 20 concurrent user activity updates...\n\n";

			std::vector<std::thread> threads;
			auto startTime = std::chrono::steady_clock::now();

			for (int i = 0; i < 20; ++i)
			{
				threads.emplace_back(handleUpdateRequest, i + 1, std::ref(pool), std::ref(totalUpdated));
			} // end of for

			for (auto& thread : threads)
			{
				thread.join();
			} // end of for

			auto endTime = std::chrono::steady_clock::now();
			auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

			std::cout << "\n✓ Completed in " << duration.count() << "ms\n";
			std::cout << "  Total rows updated: " << totalUpdated.load() << "\n";
			std::cout << "  Throughput: " << (totalUpdated.load() * 1000 / duration.count()) << " updates/sec\n\n";
		} // end of scope

		// ====================================================================
		// Example 4: Batch INSERT (High Performance)
		// ====================================================================
		std::cout << "=== Example 4: Batch INSERT (Event Logging) ===\n";
		{
			sqlite_flux::ConnectionPool pool(dbPath, 3, true);
			auto conn = pool.acquire();

			// Create events table
			conn->execute(R"(
				CREATE TABLE IF NOT EXISTS events (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					event_type TEXT NOT NULL,
					user_id INTEGER,
					timestamp INTEGER
				)
			)");

			sqlite_flux::QueryFactory factory(*conn);

			std::cout << "Inserting 1000 events using batch operation...\n";

			auto startTime = std::chrono::steady_clock::now();

			// Prepare batch insert
			auto batchInsert = factory.InsertInto("events")
				.Values({
					{"event_type", std::string("")},
					{"user_id", int64_t(0)},
					{"timestamp", int64_t(0)}
					})
				.Prepare();

			// Insert 1000 events
			for (int i = 0; i < 1000; ++i)
			{
				batchInsert.Values({
					{"event_type", std::string("page_view")},
					{"user_id", int64_t(i % 20 + 1)},
					{"timestamp", int64_t(std::time(nullptr) + i)}
					}).ExecuteBatch();
			} // end of for

			auto totalInserted = batchInsert.Finalize();

			auto endTime = std::chrono::steady_clock::now();
			auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

			std::cout << "\n✓ Inserted " << totalInserted << " events in " << duration.count() << "ms\n";
			std::cout << "  Throughput: " << (totalInserted * 1000 / duration.count()) << " inserts/sec\n";
			std::cout << "  Average: " << (duration.count() / (double)totalInserted) << "ms per insert\n\n";
		} // end of scope

		// ====================================================================
		// Example 5: Safety Demonstration
		// ====================================================================
		std::cout << "=== Example 5: Safety Mechanisms ===\n";
		{
			sqlite_flux::ConnectionPool pool(dbPath, 1, true);
			auto conn = pool.acquire();
			sqlite_flux::QueryFactory factory(*conn);

			std::cout << "1. Attempting DELETE without WHERE clause...\n";
			try
			{
				factory.DeleteFrom("users").Execute();
				std::cout << "   ✗ Should have thrown exception!\n";
			}
			catch (const std::exception& e)
			{
				std::cout << "   ✓ Prevented: " << e.what() << "\n\n";
			} // end of catch

			std::cout << "2. Attempting UPDATE without WHERE clause...\n";
			try
			{
				factory.UpdateTable("users")
					.Set("is_active", int64_t(0))
					.Execute();
				std::cout << "   ✗ Should have thrown exception!\n";
			}
			catch (const std::exception& e)
			{
				std::cout << "   ✓ Prevented: " << e.what() << "\n\n";
			} // end of catch

			std::cout << "3. Using .Unsafe() for mass operations...\n";
			auto rowsUpdated = factory.UpdateTable("users")
				.Set("is_active", int64_t(1))
				.Unsafe()
				.Execute();
			std::cout << "   ✓ Updated " << rowsUpdated << " rows (explicit .Unsafe())\n\n";
		} // end of scope

		// ====================================================================
		// Example 6: Mixed Workload (Realistic Web App)
		// ====================================================================
		std::cout << "=== Example 6: Mixed Workload (Realistic Scenario) ===\n";
		{
			sqlite_flux::ConnectionPool pool(dbPath, 10, true);

			std::cout << "Simulating 50 mixed operations (SELECT/INSERT/UPDATE)...\n\n";

			std::vector<std::thread> threads;
			std::atomic<int64_t> totalInserted{ 0 };
			std::atomic<int64_t> totalUpdated{ 0 };

			auto startTime = std::chrono::steady_clock::now();

			// Mix of operations: 60% SELECT, 30% INSERT, 10% UPDATE
			for (int i = 0; i < 50; ++i)
			{
				if (i % 10 < 6)
				{
					threads.emplace_back(handleSelectRequest, i + 1, std::ref(pool));
				}
				else if (i % 10 < 9)
				{
					threads.emplace_back(handleInsertRequest, i + 100, std::ref(pool), std::ref(totalInserted));
				}
				else
				{
					threads.emplace_back(handleUpdateRequest, i % 20 + 1, std::ref(pool), std::ref(totalUpdated));
				} // end of if-else
			} // end of for

			for (auto& thread : threads)
			{
				thread.join();
			} // end of for

			auto endTime = std::chrono::steady_clock::now();
			auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

			std::cout << "\n✓ Completed 50 operations in " << duration.count() << "ms\n";
			std::cout << "  Inserts: " << totalInserted.load() << "\n";
			std::cout << "  Updates: " << totalUpdated.load() << "\n";
			std::cout << "  Throughput: " << (50 * 1000 / duration.count()) << " ops/sec\n\n";
		} // end of scope

		std::cout << "=== All web examples completed successfully! ===\n";

		return 0;
	}
	catch (const std::exception& e)
	{
		std::cerr << "Fatal error: " << e.what() << "\n";
		return 1;
	} // end of catch
} // end of main