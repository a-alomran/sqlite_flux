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
#include <iomanip>

// Helper function to print pool statistics
void printPoolStats(const sqlite_flux::ConnectionPool& pool)
{
	std::cout << "  Pool Stats: "
		<< pool.available() << " available, "
		<< pool.inUse() << " in use "
		<< "(total: " << pool.size() << ")\n";
} // end of printPoolStats

// Simulated web request handler - SELECT
void handleSelectRequest(int requestId, sqlite_flux::ConnectionPool& pool)
{
	try
	{
		std::cout << "[SELECT " << requestId << "] Acquiring connection...\n";

		// Acquire connection from pool (RAII - auto-released)
		auto conn = pool.acquire();

		std::cout << "[SELECT " << requestId << "] ";
		printPoolStats(pool);

		// Build and execute query (thread-safe: each thread gets own QueryBuilder)
		sqlite_flux::QueryFactory factory(*conn);
		auto results = factory.FromTable("users")
			.Columns("id", "username", "email")
			.Filter("is_active", int64_t(1))
			.Limit(5)
			.Execute();

		std::cout << "[SELECT " << requestId << "] Found " << results.size() << " users\n";

		// Simulate processing time
		std::this_thread::sleep_for(std::chrono::milliseconds(10));

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

		// Simulate user registration with conflict resolution
		auto userId = factory.InsertInto("users")
			.Values({
				{"username", std::string("user_") + std::to_string(requestId)},
				{"email", std::string("user") + std::to_string(requestId) + std::string("@example.com")},
				{"is_active", int64_t(1)},
				{"created_at", int64_t(std::time(nullptr))}
				})
			.OrReplace()  // Use REPLACE if username exists
			.Execute();

		std::cout << "[INSERT " << requestId << "] Created/Updated user ID: " << userId << "\n";
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

		// Simulate deleting old sessions (safe: requires WHERE clause)
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
		std::cout << "=== sqlite_flux v1.1.0 Web Application Example ===\n";
		std::cout << "Demonstrating thread-safe operations with ConnectionPool\n\n";

		// Find database path
		// testdb.db is in databases folder copy it into bin/debug / and release
		std::string dbPath = "testdb.db";  // From build/bin/Release

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

			// Cache all schemas (thread-safe, single initialization)
			setup.cacheAllSchemas();
			std::cout << "✓ Database initialized (schema cached: " << (setup.isSchemaCached() ? "yes" : "no") << ")\n\n";
		} // end of setup scope

		// ====================================================================
		// Example 1: Pool Diagnostics & Connection Tracking
		// ====================================================================
		std::cout << "=== Example 1: Connection Pool Diagnostics ===\n";
		{
			sqlite_flux::ConnectionPool pool(dbPath, 3, true);

			std::cout << "Initial state:\n";
			printPoolStats(pool);

			std::cout << "\nAcquiring 2 connections in nested scopes...\n";
			{
				auto conn1 = pool.acquire();
				printPoolStats(pool);

				{
					auto conn2 = pool.acquire();
					printPoolStats(pool);
					std::cout << "\nReleasing conn2 (going out of scope)...\n";
				} // conn2 released here

				printPoolStats(pool);
				std::cout << "\nReleasing conn1 (going out of scope)...\n";
			} // conn1 released here

			printPoolStats(pool);
			std::cout << "\n✓ All connections returned to pool\n\n";
		} // end of scope

		// ====================================================================
		// Example 2: Concurrent SELECT Operations (Read-Heavy)
		// ====================================================================
		std::cout << "=== Example 2: Concurrent SELECT (Read-Heavy Workload) ===\n";
		{
			// Pool size smaller than threads to show connection reuse
			sqlite_flux::ConnectionPool pool(dbPath, 3, true);

			std::cout << "Pool size: " << pool.size() << " connections\n";
			std::cout << "Simulating 10 concurrent SELECT requests (connection reuse)...\n\n";

			std::vector<std::thread> threads;
			auto startTime = std::chrono::steady_clock::now();

			for (int i = 0; i < 10; ++i)
			{
				threads.emplace_back(handleSelectRequest, i + 1, std::ref(pool));
				std::this_thread::sleep_for(std::chrono::milliseconds(5));  // Stagger starts
			} // end of for

			for (auto& thread : threads)
			{
				thread.join();
			} // end of for

			auto endTime = std::chrono::steady_clock::now();
			auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

			std::cout << "\n✓ Completed in " << duration.count() << "ms\n";
			std::cout << "  Final state: ";
			printPoolStats(pool);
			std::cout << "\n";
		} // end of scope

		// ====================================================================
		// Example 3: Concurrent INSERT Operations (User Registration)
		// ====================================================================
		std::cout << "=== Example 3: Concurrent INSERT (User Registration) ===\n";
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
			if (duration.count() > 0)
			{
				std::cout << "  Throughput: " << (totalInserted.load() * 1000 / duration.count()) << " inserts/sec\n";
			}
			std::cout << "  ";
			printPoolStats(pool);
			std::cout << "\n";
		} // end of scope

		// ====================================================================
		// Example 4: Concurrent UPDATE Operations (User Activity)
		// ====================================================================
		std::cout << "=== Example 4: Concurrent UPDATE (User Activity) ===\n";
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
			if (duration.count() > 0)
			{
				std::cout << "  Throughput: " << (totalUpdated.load() * 1000 / duration.count()) << " updates/sec\n";
			}
			std::cout << "\n";
		} // end of scope

		// ====================================================================
		// Example 5: Batch INSERT (High Performance)
		// ====================================================================
		std::cout << "=== Example 5: Batch INSERT (Event Logging) ===\n";
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

			// Prepare batch insert (uses prepared statements internally)
			auto batchInsert = factory.InsertInto("events")
				.Values({
					{"event_type", std::string("")},
					{"user_id", int64_t(0)},
					{"timestamp", int64_t(0)}
					})
				.Prepare();

			// Insert 1000 events in a transaction
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
			if (duration.count() > 0)
			{
				std::cout << "  Throughput: " << (totalInserted * 1000 / duration.count()) << " inserts/sec\n";
			}
			std::cout << "  Average: " << std::fixed << std::setprecision(3)
				<< (duration.count() / (double)totalInserted) << "ms per insert\n\n";
		} // end of scope

		// ====================================================================
		// Example 6: AsyncExecutor (Asynchronous Operations)
		// ====================================================================
		std::cout << "=== Example 6: AsyncExecutor (Async/Await Pattern) ===\n";
		{
			sqlite_flux::ConnectionPool pool(dbPath, 5, true);
			sqlite_flux::AsyncExecutor async(pool, 4);  // 4 worker threads

			std::cout << "Executing async operations with " << async.availableConnections() << " connections...\n\n";

			auto startTime = std::chrono::steady_clock::now();

			// Launch async queries
			auto countTask = async.count("users");
			auto selectTask = async.selectAll("users");
			auto existsTask = async.exists("users", "is_active = 1");

			// Get results
			auto userCount = countTask.get();
			auto allUsers = selectTask.get();
			auto hasActive = existsTask.get();

			auto endTime = std::chrono::steady_clock::now();
			auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

			std::cout << "✓ Async operations completed in " << duration.count() << "ms\n";
			std::cout << "  User count: " << userCount << "\n";
			std::cout << "  Fetched " << allUsers.size() << " users\n";
			std::cout << "  Has active users: " << (hasActive ? "yes" : "no") << "\n";
			std::cout << "  Pending operations: " << async.pendingOperations() << "\n\n";
		} // end of scope

		// ====================================================================
		// Example 7: Safety Mechanisms
		// ====================================================================
		std::cout << "=== Example 7: Safety Mechanisms ===\n";
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
		// Example 8: Mixed Workload (Realistic Web App)
		// ====================================================================
		std::cout << "=== Example 8: Mixed Workload (Realistic Scenario) ===\n";
		{
			sqlite_flux::ConnectionPool pool(dbPath, 10, true);

			std::cout << "Simulating 50 mixed operations (60% SELECT, 30% INSERT, 10% UPDATE)...\n";
			std::cout << "Initial state: ";
			printPoolStats(pool);
			std::cout << "\n";

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
			if (duration.count() > 0)
			{
				std::cout << "  Throughput: " << (50 * 1000 / duration.count()) << " ops/sec\n";
			}
			std::cout << "  Final state: ";
			printPoolStats(pool);
			std::cout << "\n";
		} // end of scope

		// ====================================================================
		// Example 9: Schema Caching Performance
		// ====================================================================
		std::cout << "=== Example 9: Schema Caching Performance ===\n";
		{
			sqlite_flux::ConnectionPool pool(dbPath, 1, true);
			auto conn = pool.acquire();

			// First query - schema cached during pool initialization
			auto start1 = std::chrono::high_resolution_clock::now();
			for (int i = 0; i < 1000; ++i)
			{
				auto schema = conn->getTableSchema("users");
			}
			auto end1 = std::chrono::high_resolution_clock::now();
			auto duration1 = std::chrono::duration_cast<std::chrono::microseconds>(end1 - start1);

			std::cout << "✓ 1000 schema lookups (cached): " << duration1.count() << "μs\n";
			std::cout << "  Average: " << (duration1.count() / 1000.0) << "μs per lookup\n";
			std::cout << "  Schema is cached: " << (conn->isSchemaCached() ? "yes" : "no") << "\n\n";
		} // end of scope

		std::cout << "=== All examples completed successfully! ===\n";
		std::cout << "\nThread-Safety Features Demonstrated:\n";
		std::cout << "  ✓ ConnectionPool with atomic connection tracking\n";
		std::cout << "  ✓ RAII connection guards (automatic release)\n";
		std::cout << "  ✓ Thread-safe schema caching\n";
		std::cout << "  ✓ Concurrent read/write operations\n";
		std::cout << "  ✓ Safety mechanisms for mass operations\n";
		std::cout << "  ✓ Async/await pattern with AsyncExecutor\n";

		return 0;
	}
	catch (const std::exception& e)
	{
		std::cerr << "Fatal error: " << e.what() << "\n";
		return 1;
	} // end of catch
} // end of main