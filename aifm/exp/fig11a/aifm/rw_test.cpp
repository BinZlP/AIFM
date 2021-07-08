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

#include "profile.hpp"
extern unsigned long long prefetch_time, prefetch_count;
extern unsigned long long swapin_time, swapin_count;
unsigned long long deref_time=0, deref_count=0;

constexpr uint64_t kCacheSize = 13312 * Region::kSize;
constexpr uint64_t kFarMemSize = 40ULL << 30;
constexpr uint64_t kNumGCThreads = 15;
constexpr uint64_t kNumConnections = 600;
//constexpr uint64_t kUncompressedFileSize = 16*(1ULL<<30);
constexpr uint64_t kUncompressedFileSize = 16000000000;
constexpr uint64_t kUncompressedFileNumBlocks =
    ((kUncompressedFileSize - 1) / snappy::FileBlock::kSize) + 1;
constexpr uint32_t kNumUncompressedFiles = 16;
constexpr bool kUseTpAPI = false;

using namespace std;

alignas(4096) snappy::FileBlock file_block;
//std::unique_ptr<Array<snappy::FileBlock, kUncompressedFileNumBlocks>>
//    fm_array_ptrs[kNumUncompressedFiles];

std::unique_ptr<Array<snappy::FileBlock, kUncompressedFileNumBlocks>>
    fm_array_ptr;

void write_file_to_string(const string &file_path, const string &str) {
  std::ofstream fs(file_path);
  fs << str;
  fs.close();
}

/*
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
*/

void my_flush_cache() {
  fm_array_ptr->disable_prefetch();
  for(uint64_t i=0; i<kUncompressedFileNumBlocks; i++) {
    file_block = fm_array_ptr->read(i);
    ACCESS_ONCE(file_block.data[0]);
  }
  fm_array_ptr->enable_prefetch();
}

/*
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
      cerr << "Have read " << sum << " bytes." << endl;
    }
  }
  if (sum != kUncompressedFileSize) {
    helpers::dump_core();
  }

  // Flush the cache to ensure there's no pending dirty data.
  flush_cache();

  close(fd);
}
*/

void generate_fm_array() {
  snappy::FileBlock blk;
  for (uint32_t i=0; i<snappy::FileBlock::kSize; i++) {
    blk.data[i] = 0;
  }

  uint64_t sum = 0;
  while ( sum < kUncompressedFileSize) {
    /*for(uint32_t i=0; i<kNumUncompressedFiles; i++) {
      DerefScope scope;
      fm_array_ptrs[i]->at_mut(scope, sum/snappy::FileBlock::kSize) = blk;
    }*/

    DerefScope scope;
    fm_array_ptr->at_mut(scope, sum/snappy::FileBlock::kSize) = blk;

    sum += snappy::FileBlock::kSize;
    if ( (sum%(1ULL<<30)) == 0 )
      cout << "Wrote " << sum << " bytes." << endl;
  }

  my_flush_cache();
}

void fm_compress_files_bench(const string &in_file_path,
                             const string &out_file_path) {
  string out_str;
  //read_files_to_fm_array(in_file_path);
  generate_fm_array();
  cout << "Generated far memory" << endl;

  struct timespec local_time[2];

  // ********************** READ **********************
  auto start = chrono::steady_clock::now();
  /*for (uint32_t i = 0; i < kNumUncompressedFiles; i++) {
    // std::cout << "Compressing file " << i << std::endl;
    // snappy::Compress<kUncompressedFileNumBlocks, kUseTpAPI>(
    //     fm_array_ptrs[i].get(), kUncompressedFileSize, &out_str);
    //std::cout << "Reading array " << i << std::endl;
    for (uint32_t j = 0; j < kUncompressedFileNumBlocks; j++) {
        clock_gettime(CLOCK_MONOTONIC, &local_time[0]);

        file_block = fm_array_ptrs[i]->read(j);

        clock_gettime(CLOCK_MONOTONIC, &local_time[1]);
        calclock(local_time, &deref_time, &deref_count);
    }

  }*/
  for (uint64_t i = 0; i < kUncompressedFileNumBlocks; i++) {
    //clock_gettime(CLOCK_MONOTONIC, &local_time[0]);

    file_block = fm_array_ptr->read(i);

    //clock_gettime(CLOCK_MONOTONIC, &local_time[1]);
    //calclock(local_time, &deref_time, &deref_count);
  }

  auto end = chrono::steady_clock::now();

/*
  cout << "Read #1" << endl;
  cout << "Elapsed time in microseconds : "
       << chrono::duration_cast<chrono::microseconds>(end - start).count()
       << " µs" << endl;
  start = chrono::steady_clock::now();
  for (uint64_t i=0; i<kUncompressedFileNumBlocks; i++)
    file_block = fm_array_ptr->read(i);
  end = chrono::steady_clock::now();
  cout << "Read #2" << endl;
  cout << "Elapsed time in microseconds : "
       << chrono::duration_cast<chrono::microseconds>(end - start).count()
       << " µs" << endl;
  start = chrono::steady_clock::now();
  for (uint64_t i=0; i<kUncompressedFileNumBlocks; i++)
    file_block = fm_array_ptr->read(i);
  end = chrono::steady_clock::now();
  cout << "Read #3" << endl;
  cout << "Elapsed time in microseconds : "
       << chrono::duration_cast<chrono::microseconds>(end - start).count()
       << " µs" << endl;
  start = chrono::steady_clock::now();
  for (uint64_t i=0; i<kUncompressedFileNumBlocks; i++)
    file_block = fm_array_ptr->read(i);
  end = chrono::steady_clock::now();
  cout << "Read #4" << endl;
  cout << "Elapsed time in microseconds : "
       << chrono::duration_cast<chrono::microseconds>(end - start).count()
       << " µs" << endl;
  start = chrono::steady_clock::now();
  for (uint64_t i=0; i<kUncompressedFileNumBlocks; i++)
    file_block = fm_array_ptr->read(i);
  end = chrono::steady_clock::now();
  cout << "Read #5" << endl;
  cout << "Elapsed time in microseconds : "
       << chrono::duration_cast<chrono::microseconds>(end - start).count()
       << " µs" << endl;
*/



  cout << "*** READ RESULT ***" << endl;
  cout << "Elapsed time in microseconds : "
       << chrono::duration_cast<chrono::microseconds>(end - start).count()
       << " µs" << endl;

#ifdef PROFILE
  cout << endl << "[Prefetch statistics]" << endl
    << "  Prefetch total time: " << prefetch_time << endl
    << "  Prefetch count: " << prefetch_count << endl
    << "  Prefetch avg. time: " << prefetch_time/prefetch_count << endl;

  cout << endl << "[Swap-in statistics]" << endl
    << "  Swap-in total time: " << swapin_time << endl
    << "  Swap-in cout: " << swapin_count << endl;

#endif

  /*
  cout << endl << "[Dereference statistics]" << endl
    << "  Deref. total time: " << deref_time << endl
    << "  Deref. count: " << deref_count << endl
    << "  Deref. avg. time: " << deref_time/deref_count << endl;
  */

  // ******************* READ END **********************

  /*
  // ******************* WRITE *************************
  snappy::FileBlock write_block;
  for (uint32_t i = 0; i < write_block.kSize; i++)
    write_block.data[i] = 1;

  start = chrono::steady_clock::now();
  for (uint32_t i = 0; i < kNumUncompressedFiles; i++) {
    // std::cout << "Writing array" << i << std::endl;
    for(uint32_t j = 0; j < kUncompressedFileNumBlocks; j++) {
      DerefScope scope;
      fm_array_ptrs[i]->at_mut(scope, j) =
        write_block;
    }
  }
  end = chrono::steady_clock::now();
  cout << "*** WRITE RESULT ***" << endl;
  cout << "Elapsed time in microseconds : "
    << chrono::duration_cast<chrono::microseconds>(end - start).count()
    << " µs" << endl;
  // ****************** WRITE END **********************
  */

  // write_file_to_string(out_file_path, out_str);
}

void do_work(netaddr raddr) {
  auto manager = std::unique_ptr<FarMemManager>(FarMemManagerFactory::build(
      kCacheSize, kNumGCThreads,
      new TCPDevice(raddr, kNumConnections, kFarMemSize)));
  /*for (uint32_t i = 0; i < kNumUncompressedFiles; i++) {
    fm_array_ptrs[i].reset(
        manager->allocate_array_heap<snappy::FileBlock,
                                     kUncompressedFileNumBlocks>());
  }*/
  fm_array_ptr.reset(manager->allocate_array_heap<snappy::FileBlock, kUncompressedFileNumBlocks>());
//  fm_compress_files_bench("/mnt/enwik9.uncompressed",
//                          "/mnt/enwik9.compressed.tmp");
  fm_compress_files_bench("./enwik9.uncompressed",
                          "./enwik9.compressed.tmp");

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
