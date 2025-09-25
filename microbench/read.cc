#include <cassert>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <variant>
#include <vector>

// Remus core ------------
#include <romulus/cfg.h>
#include <romulus/compute_node.h>
#include <romulus/compute_thread.h>
#include <romulus/logging.h>
#include <romulus/mem_node.h>
#include <romulus/util.h>
#include <unistd.h>

int main(int argc, char **argv) {
  romulus::INIT();

  // Configure and parse the arguments
  auto args = std::make_shared<romulus::ArgMap>();
  args->import(romulus::ARGS);
  args->import(CASPAXOS_ARGS);
  args->parse(argc, argv);

  // Extract the args we need in EVERY node
  uint64_t id = args->uget(romulus::NODE_ID);
  uint64_t m0 = args->uget(romulus::FIRST_MN_ID);
  uint64_t mn = args->uget(romulus::LAST_MN_ID);
  uint64_t c0 = args->uget(romulus::FIRST_CN_ID);
  uint64_t cn = args->uget(romulus::LAST_CN_ID);

  // prepare network information about this machine and about memnodes
  romulus::MachineInfo self(id, id_to_dns_name(id));
  std::vector<romulus::MachineInfo> memnodes;
  for (uint64_t i = m0; i <= mn; ++i) {
    memnodes.emplace_back(i, id_to_dns_name(i));
  }

  // Information needed if this machine will operate as a memory node
  std::unique_ptr<romulus::MemoryNode> memory_node;

  // Information needed if this machine will operate as a compute node
  std::shared_ptr<romulus::ComputeNode> compute_node;

  // Memory Node configuration must come first!
  if (id >= m0 && id <= mn) {
    // Make the pools, await connections
    memory_node.reset(new romulus::MemoryNode(self, args));
  }

  // Configure this to be a Compute Node?
  if (id >= c0 && id <= cn) {
    compute_node.reset(new romulus::ComputeNode(self, args));
    // NB:  If this ComputeNode is also a MemoryNode, then we need to pass the
    //      rkeys to the local MemoryNode.  There's no harm in doing them first.
    if (memory_node.get() != nullptr) {
      auto rkeys = memory_node->get_local_rkeys();
      compute_node->connect_local(memnodes, rkeys);
    }
    compute_node->connect_remote(memnodes);
  }

  // If this is a memory node, pause until it has received all the connections
  // it's expecting, then spin until the control channel in each segment
  // becomes 1. Then, shutdown the memory node.
  if (memory_node) {
    memory_node->init_done();
  }

  std::vector<std::shared_ptr<AsyncComputeThread<State>>> compute_threads;
  uint64_t total_threads = (cn - c0 + 1) * args->uget(romulus::CN_THREADS);
  // Number of cloudlab nodes involved in consensus
  uint64_t system_size = (cn - c0 + 1);
  if (id >= c0 && id <= cn) {
    for (uint64_t i = 0; i < args->uget(romulus::CN_THREADS); ++i) {
      compute_threads.push_back(
          std::make_shared<AsyncComputeThread<State>>(id, compute_node, args));
    }

    ROMULUS_INFO("Starting Cas-Paxos experiment with {} threads",
                 total_threads);
    if (id == 0) {
      // Make all compute threads aware of the root
      compute_threads[0]->set_root(
          compute_threads[0]->allocate<romulus::rdma_ptr<State>>(
              total_threads));
    }

    // Create threads to allocate and write concurrently
    std::vector<std::thread> worker_threads;

    // ROMULUS_ASSERT(total_threads == system_size,
    //              "Only supports single-threaded at this time.");
    ROMULUS_ASSERT(total_threads == args->uget(romulus::CN_OPS_PER_THREAD),
                   "Must have batch size equal to system size.");
    ROMULUS_ASSERT(args->sget(romulus::QP_SCHED_POL) == "ONE_TO_ONE",
                   "Only supports ONE_TO_ONE scheduling policy at this time.");

    std::vector<std::shared_ptr<util::Metrics>> metric_total;
    metric_total.resize(compute_threads.size());

    // Launch worker threads
    for (auto &t : compute_threads) {
      worker_threads.push_back(std::thread([t, total_threads, system_size, id,
                                            args, &metric_total]() {
        t->arrive_control_barrier(total_threads);
        std::shared_ptr<util::Metrics> metrics =
            std::make_shared<util::Metrics>();
        ROMULUS_DEBUG("[MAIN BARRIER] Start init...");
        // ------------- RDMA ALLOCATIONS -------------
        // Every cloudlab node in the system will have the 0th ComputeThread
        // perform the allocations and key exchanges before the experiment
        uint64_t payload_size = args->uget(PAYLOAD_SIZE);
        uint64_t capacity = args->uget(CAPACITY);
        // First allocation for the RDMA memory:
        // +-----------------------------------+
        // |   Scratch    -- system Size       |
        // +-----------------------------------+

        // +-----------------------------------+
        // |   Proposed   -- capacity          |
        // +-----------------------------------+
        // |   Log        -- capacity          |
        // +-----------------------------------+
        // |   Buf        -- payload size      |
        // +-----------------------------------+
        romulus::rdma_ptr<State> peer_raw =
            t->allocate<State>((2 * capacity) + payload_size);
        // Right now, each node has a singular buffer for payloads of
        // payload size bytes This is for brevity, ideally each log slot
        // would have its own corresponding buffer region

        // Write our bundle to corresponding index
        romulus::rdma_ptr<romulus::rdma_ptr<State>> base =
            t->get_root<romulus::rdma_ptr<State>>();
        t->Write(base + (id * args->uget(romulus::CN_THREADS)) + t->get_tid(),
                 peer_raw);
        // Sync here after the all the threads finish allocation phase
        t->arrive_control_barrier(total_threads);
        ROMULUS_DEBUG("[MAIN BARRIER] Init end. Starting experiment...");

        Driver driver(metrics, t, total_threads, system_size, args);
        // Perform latency test
        driver.RUN();
        t->arrive_control_barrier(total_threads);
        // Store per-thread metrics in the global metrics vector
        metric_total.at(t->get_tid()) = metrics;
        ROMULUS_DEBUG("[MAIN BARRIER] Done.");
      }));
    }
    // Wait for all threads to complete
    for (auto &t : worker_threads) {
      t.join();
    }
    if (id == 0) {
      util::Metrics total;
      for (const auto &m : metric_total) {
        if (m) {
          total += *m;
        }
      }
      util::Results r;
      util::calc(r, total);
      ROMULUS_INFO("Experiment results:");
      util::print(r, total);
      util::log_csv(r, total, "results.csv");
    }
    ROMULUS_INFO("Cas-Paxos experiment completed successfully.");
  }
  return 0;
}