extern "C" {
#include <runtime/runtime.h>
}

#include "snappy.h"

#include "array.hpp"
#include "deref_scope.hpp"
#include "device.hpp"
#include "helpers.hpp"
#include "manager.hpp"

#include <algorithm>
#include <chrono>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <streambuf>
#include <string>
#include <unistd.h>

#ifdef PROFILE
extern unsigned long long pref_swapin_time, pref_swapin_count, pref_swapin_size;
extern unsigned long long swapin_time, swapin_count, swapin_size;
extern unsigned long long totalref_count, remoteref_count;
#endif

constexpr uint64_t kCacheSize = 13312 * Region::kSize;
constexpr uint64_t kFarMemSize = 20ULL << 30;
constexpr uint64_t kNumGCThreads = 15;
constexpr uint64_t kNumConnections = 600;
constexpr uint64_t kUncompressedFileSize = 1000000000;
constexpr uint64_t kUncompressedFileNumBlocks =
    ((kUncompressedFileSize - 1) / snappy::FileBlock::kSize) + 1;
constexpr uint32_t kNumUncompressedFiles = 16;
constexpr bool kUseTpAPI = false;

using namespace std;

alignas(4096) snappy::FileBlock file_block;
std::unique_ptr<Array<snappy::FileBlock, kUncompressedFileNumBlocks>>
    fm_array_ptrs[kNumUncompressedFiles];

void write_file_to_string(const string &file_path, const string &str) {
  std::ofstream fs(file_path);
  fs << str;
  fs.close();
}

void flush_cache() {
  for (uint32_t k = 0; k < kNumUncompressedFiles; k++) {
    fm_array_ptrs[k]->disable_prefetch();
  }
  for (uint32_t i = 0; i < kUncompressedFileNumBlocks; i++) {
    for (uint32_t k = 0; k < kNumUncompressedFiles; k++) {
      file_block = fm_array_ptrs[k]->read(i);
      ACCESS_ONCE(file_block.data[0]);
    }
  }
  for (uint32_t k = 0; k < kNumUncompressedFiles; k++) {
    fm_array_ptrs[k]->enable_prefetch();
  }
}

void read_files_to_fm_array(const string &in_file_path) {
  int fd = open(in_file_path.c_str(), O_RDONLY | O_DIRECT);
  if (fd == -1) {
    helpers::dump_core();
  }
  // Read file and save data into the far-memory array.
  int64_t sum = 0, cur = snappy::FileBlock::kSize, tmp;
  while (sum != kUncompressedFileSize) {
    BUG_ON(cur != snappy::FileBlock::kSize);
    cur = 0;
    while (cur < (int64_t)snappy::FileBlock::kSize) {
      tmp = read(fd, file_block.data + cur, snappy::FileBlock::kSize - cur);
      if (tmp <= 0) {
        break;
      }
      cur += tmp;
    }
    for (uint32_t i = 0; i < kNumUncompressedFiles; i++) {
      DerefScope scope;
      fm_array_ptrs[i]->at_mut(scope, sum / snappy::FileBlock::kSize) =
          file_block;
    }
    sum += cur;
    if ((sum % (1 << 20)) == 0) {
      //cerr << "Have read " << sum << " bytes." << endl;
    }
  }
  if (sum != kUncompressedFileSize) {
    helpers::dump_core();
  }

  // Flush the cache to ensure there's no pending dirty data.
  flush_cache();

  close(fd);
}

/*
template<uint64_t kNumBlocks>
void read_all(Array<FileBlock, kNumBlocks> *fm_array_ptr) {
  for(uint32_t i=0; i<kUncompressedFileNumBlocks; i++) {
    ACCESS_ONCE(fm_array_ptr->read(i).data[0]);
  }
}*/

void fm_compress_files_bench(const string &in_file_path,
                             const string &out_file_path) {
  string out_str;
  read_files_to_fm_array(in_file_path);
  //snappy::FileBlock tmp;

#ifdef PROFILE
  pref_swapin_time = 0;
  pref_swapin_size = 0;
  pref_swapin_count = 0;

  swapin_time = 0;
  swapin_size = 0;
  swapin_count = 0;

  totalref_count=0;
  remoteref_count=0;
#endif

  auto start = chrono::steady_clock::now();
  for (uint32_t i = 0; i < kNumUncompressedFiles; i++) {
    /*std::cout << "Compressing file " << i << std::endl;
    snappy::Compress<kUncompressedFileNumBlocks, kUseTpAPI>(
        fm_array_ptrs[i].get(), kUncompressedFileSize, &out_str);*/
    //fm_array_ptrs[i].get();
    for (uint32_t j = 0; j < kUncompressedFileNumBlocks; j++) {
    //  tmp = fm_array_ptrs[i].get()->read(j);
        file_block = fm_array_ptrs[i].get()->read(j);
    }
  }
  auto end = chrono::steady_clock::now();
  cout << "Elapsed time in microseconds : "
       << chrono::duration_cast<chrono::microseconds>(end - start).count()
       << " µs" << endl;

#ifdef PROFILE
  cout << endl << "[Dereference statistics]" << endl
    << "  Total ref. count: " << totalref_count << endl
    << "  Remote ref. count: " << remoteref_count << endl;

  cout << endl << "[Prefetch statistics]" << endl
    << "  Prefetch swap-in total time: " << pref_swapin_time << endl
    << "  Prefetch swap-in count: " << pref_swapin_count << endl
    << "  Prefetch swap-in size: " << pref_swapin_size << endl;


  cout << endl << "[Swap-in statistics]" << endl
    << "  Swap-in total time: " << swapin_time << endl
    << "  Swap-in count: " << swapin_count << endl
    << "  Swap-in size: " << swapin_size << endl;
  cout << endl << endl;

#endif


  // write_file_to_string(out_file_path, out_str);
}

void do_work(netaddr raddr) {
  auto manager = std::unique_ptr<FarMemManager>(FarMemManagerFactory::build(
      kCacheSize, kNumGCThreads,
      new TCPDevice(raddr, kNumConnections, kFarMemSize)));
  for (uint32_t i = 0; i < kNumUncompressedFiles; i++) {
    fm_array_ptrs[i].reset(
        manager->allocate_array_heap<snappy::FileBlock,
                                     kUncompressedFileNumBlocks>());
  }
  fm_compress_files_bench("/mnt/enwik9.uncompressed",
                          "/mnt/enwik9.compressed.tmp");

  std::cout << "Force existing..." << std::endl;
  exit(0);
}

int argc;
void my_main(void *arg) {
  char **argv = (char **)arg;
  std::string ip_addr_port(argv[1]);
  do_work(helpers::str_to_netaddr(ip_addr_port));
}

int main(int _argc, char *argv[]) {
  int ret;

  if (_argc < 3) {
    std::cerr << "usage: [cfg_file] [ip_addr:port]" << std::endl;
    return -EINVAL;
  }

  char conf_path[strlen(argv[1]) + 1];
  strcpy(conf_path, argv[1]);
  for (int i = 2; i < _argc; i++) {
    argv[i - 1] = argv[i];
  }
  argc = _argc - 1;

  ret = runtime_init(conf_path, my_main, argv);
  if (ret) {
    std::cerr << "failed to start runtime" << std::endl;
    return ret;
  }

  return 0;
}
