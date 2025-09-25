#include <gtest/gtest.h>

#include <unistd.h>
#include <algorithm>
#include <memory>
#include <vector>
#include <unistd.h>


// Romulus core ------------
#include <romulus/compute_node.h>
#include <romulus/compute_thread.h>
#include <romulus/logging.h>
#include <romulus/mem_node.h>
#include <romulus/util.h>
#include <romulus/cloudlab.h>

int main(int argc, char **argv) {
  romulus::INIT();

  // Configure and parse the arguments
  auto args = std::make_shared<romulus::ArgMap>();
  args->import(romulus::ARGS);
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

    ROMULUS_INFO("Starting test with {} threads",
                 total_threads);

    // Create threads to allocate and write concurrently
    std::vector<std::thread> worker_threads;
    // Launch worker threads
    for (auto &t : compute_threads) {
      worker_threads.push_back(std::thread(
          [t, total_threads, system_size, id, args, &metric_total]() {
            t->arrive_control_barrier(total_threads);
            // TODO
          }));
    }
    // Wait for all threads to complete
    for (auto &t : worker_threads) {
      t.join();
    }
    ROMULUS_INFO("Done.");
    return 0;
  }