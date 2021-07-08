extern "C" {
#include <runtime/runtime.h>
}

#include "snappy.h"

#include "array.hpp"
#include "deref_scope.hpp"
#include "device.hpp"
#include "helpers.hpp"
#include "manager.hpp"

#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <numa.h>
#include <streambuf>
#include <string>
#include <unistd.h>

using namespace std;

constexpr uint32_t kUncompressedFileSize = 1000000000;
constexpr const uint32_t kNumUncompressedFiles = 16;
constexpr uint64_t kUncompressedFileNumBlocks =
    ((kUncompressedFileSize - 1) / snappy::FileBlock::kSize) + 1;

void *buffers[kNumUncompressedFiles - 1];

alignas(4096) snappy::FileBlock file_block;
snappy::FileBlock* arrays[kNumUncompressedFiles];


void generate_array() {
  for(int i=0; i<kNumUncompressedFiles; i++) {
    arrays[i] = (snappy::FileBlock *)calloc(snappy::FileBlock::kSize, kUncompressedFileNumBlocks);
  }

  snappy::FileBlock blk;
  for (uint32_t i=0; i<snappy::FileBlock::kSize; i++) {
    blk.data[i] = 0;
  }

  uint64_t sum = 0;
  while ( sum < kUncompressedFileSize) {
    for(uint32_t j=0; j<kUncompressedFileNumBlocks; j++) {
    for(uint32_t i=0; i<kNumUncompressedFiles; i++) {
      arrays[i][j] = blk;
    }
    }

    //DerefScope scope;
    //fm_array_ptr->at_mut(scope, sum/snappy::FileBlock::kSize) = blk;

    sum += snappy::FileBlock::kSize;
    if ( (sum%(1ULL<<30)) == 0 )
      cout << "Wrote " << sum << " bytes." << endl;
  }
}






void read_files_to_block_array(const string &in_file_path) {
  int fd = open(in_file_path.c_str(), O_RDONLY | O_DIRECT);
  if (fd == -1) {
    helpers::dump_core();
  }

  for(int i=0; i<kNumUncompressedFiles; i++) {
    arrays[i] = (snappy::FileBlock *)calloc(snappy::FileBlock::kSize, kUncompressedFileNumBlocks);
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
      arrays[i][cur/snappy::FileBlock::kSize] = file_block;
    }
    sum += cur;
    if ((sum % (1 << 20)) == 0) {
      cerr << "Have read " << sum << " bytes." << endl;
    }
  }
  if (sum != kUncompressedFileSize) {
    helpers::dump_core();
  }

  close(fd);
}


string read_file_to_string(const string &file_path) {
  ifstream fs(file_path);
  auto guard = helpers::finally([&]() { fs.close(); });
  return string((std::istreambuf_iterator<char>(fs)),
                std::istreambuf_iterator<char>());
}

void write_file_to_string(const string &file_path, const string &str) {
  std::ofstream fs(file_path);
  fs << str;
  fs.close();
}

void compress_file(const string &in_file_path, const string &out_file_path) {
  string in_str = read_file_to_string(in_file_path);
  string out_str;
  auto start = chrono::steady_clock::now();
  snappy::Compress(in_str.data(), in_str.size(), &out_str);
  auto end = chrono::steady_clock::now();
  cout << "*** Read result ***" << endl;
  cout << "Elapsed time in microseconds : "
       << chrono::duration_cast<chrono::microseconds>(end - start).count()
       << " µs" << endl;
  write_file_to_string(out_file_path, out_str);
}


void compress_files_bench(const string &in_file_path,
                          const string &out_file_path) {
  //read_files_to_block_array(in_file_path);
  generate_array();

  /*for (uint32_t i = 0; i < kNumUncompressedFiles - 1; i++) {
    buffers[i] = numa_alloc_onnode(kUncompressedFileSize, 1);
    if (buffers[i] == nullptr) {
      helpers::dump_core();
    }
    memcpy(buffers[i], in_str.data(), in_str.size());
  }*/

  auto start = chrono::steady_clock::now();
  /*for (uint32_t i = 0; i < kNumUncompressedFiles; i++) {
    std::cout << "Compressing file " << i << std::endl;
    if (i == 0) {
      snappy::Compress(in_str.data(), in_str.size(), &out_str);
    } else {
      snappy::Compress((const char *)buffers[i - 1], kUncompressedFileSize,
                       &out_str);
    }
  }*/

  for (int i=0; i<kNumUncompressedFiles; i++) {
    for (int j=0; j<kUncompressedFileNumBlocks; j++){
      file_block = arrays[i][j];
      for(int k=0; k<1600; k++);
    }
  }

  auto end = chrono::steady_clock::now();
  cout << "*** Read complete ***" << endl;
  cout << "Elapsed time in microseconds : "
       << chrono::duration_cast<chrono::microseconds>(end - start).count()
       << " µs" << endl;


/*  snappy::FileBlock write_block;
  for (int i=0; i<snappy::FileBlock::kSize; i++)
    write_block.data[i]=1;

  start = chrono::steady_clock::now();
  for (int i=0; i < kNumUncompressedFiles; i++) {
    for(int j=0; j < kUncompressedFileNumBlocks; j++) {
      arrays[i][j] = write_block;
    }
  }
  end = chrono::steady_clock::now();
  cout << "*** Write complete ***" << endl;
  cout << "Elapsed time in microseconds : "
       << chrono::duration_cast<chrono::microseconds>(end - start).count()
       << " µs" << endl;
*/
  /*
  for (uint32_t i = 0; i < kNumUncompressedFiles - 1; i++) {
    numa_free(buffers[i], kUncompressedFileSize);
  }*/

  // write_file_to_string(out_file_path, out_str);
}

void do_work(void *arg) {
  compress_files_bench("/mnt/enwik9.uncompressed",
                       "/mnt/enwik9.compressed.tmp");
}

int main(int argc, char *argv[]) {
  int ret;

  if (argc < 2) {
    std::cerr << "usage: [cfg_file]" << std::endl;
    return -EINVAL;
  }

  ret = runtime_init(argv[1], do_work, NULL);
  if (ret) {
    std::cerr << "failed to start runtime" << std::endl;
    return ret;
  }

  return 0;
}
