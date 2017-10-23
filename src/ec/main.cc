#include "common/platform.h"

#include <cassert>
#include <iostream>
#include <vector>
#include <fcntl.h>
#include <unistd.h>

#include "common/reed_solomon.h"
#include "common/time_utils.h"

#define MFSBLOCKSIZE 65536

void encode_parity(std::vector<std::vector<uint8_t>> &output,
		const std::vector<std::vector<uint8_t>> &input, int m) {
	int size = input[0].size();

	output.resize(m);
	for (int i = 0; i < m; ++i) {
		output[i].assign(size, 0xFF);
	}

	ReedSolomon<32, 32> rs(input.size(), m);
	ReedSolomon<32, 32>::ConstFragmentMap data_fragments{{0}};
	ReedSolomon<32, 32>::FragmentMap parity_fragments{{0}};

	for (int i = 0; i < (int)input.size(); ++i) {
		data_fragments[i] = input[i].data();
	}
	for (int i = 0; i < m; ++i) {
		parity_fragments[i] = output[i].data();
	}

	rs.encode(data_fragments, parity_fragments, size);
}

void recover_parts(std::vector<std::vector<uint8_t>> &output,
		const ReedSolomon<32, 32>::ErasedMap erased,
		const ReedSolomon<32, 32>::ErasedMap zero_input,
		const std::vector<std::vector<uint8_t>> &data,
		const std::vector<std::vector<uint8_t>> &parity) {
	ReedSolomon<32, 32> rs(data.size(), parity.size());
	ReedSolomon<32, 32>::ConstFragmentMap input_fragments{{0}};
	ReedSolomon<32, 32>::FragmentMap output_fragments{{0}};
	int size = data[0].size();
	int parts_count = data.size() + parity.size();

	output.resize(erased.count());
	for (int i = 0; i < (int)output.size(); ++i) {
		output[i].assign(size, 0xFF);
	}

	for (int i = 0; i < (int)data.size(); ++i) {
		if (!zero_input[i]) {
			input_fragments[i] = data[i].data();
		}
	}
	for (int i = 0; i < (int)parity.size(); ++i) {
		if (!zero_input[data.size() + i]) {
			input_fragments[data.size() + i] = parity[i].data();
		}
	}

	int output_index = 0;
	for (int i = 0; i < parts_count; ++i) {
		if (erased[i]) {
			output_fragments[i] = output[output_index].data();
			++output_index;
		}
	}

	rs.recover(input_fragments, erased, output_fragments, size);
}

int main(int argc, char **argv)
{
	if(argc != 3)
	{
		std::cout << "Usage: ./ec k m" << std::endl;
		return 0;
	}
	int k = std::atoi(argv[1]);
	int m = std::atoi(argv[2]);

	std::vector<std::vector<uint8_t>> data, parity, recovered;
	ReedSolomon<32, 32>::ErasedMap erased, zero_input;

	data.resize(k);
	parity.resize(m);

	for(int i = 0; i < k; i++)
	{
		uint8_t buffer[MFSBLOCKSIZE];
		std::string path = "data." + std::to_string(i);
		int fd = open(path.c_str(), O_RDONLY);
		if (fd > 0)
		{
			pread(fd, buffer, MFSBLOCKSIZE, 4);
			data[i].resize(MFSBLOCKSIZE);
			data[i].assign(buffer, buffer + MFSBLOCKSIZE);
		}
		else
		{
			zero_input.set(i);
			erased.set(i);
			data[i].resize(MFSBLOCKSIZE);
		}
		
	}
	
	for(int i = 0; i < m; i++)
	{
		uint8_t buffer[MFSBLOCKSIZE];
		std::string path = "parity." + std::to_string(i);
		int fd = open(path.c_str(), O_RDONLY);
		if (fd > 0)
		{
			pread(fd, buffer, MFSBLOCKSIZE, 4);
			parity[i].resize(MFSBLOCKSIZE);
			parity[i].assign(buffer, buffer + MFSBLOCKSIZE);
		}
		else
		{
			zero_input.set(k + i);
			erased.set(k + i);
			parity[i].resize(MFSBLOCKSIZE);
		}
	}	

	recover_parts(recovered, erased, zero_input, data, parity);

	int recover_index = 0;
	int fd = open("recover", O_RDWR | O_TRUNC | O_CREAT, 0666);
	for(int i = 0; i < k; i++)
	{
		if(!zero_input[i])
		{
			pwrite(fd, data[i].data(), MFSBLOCKSIZE, i * MFSBLOCKSIZE);
		}
		else
		{
			pwrite(fd, recovered[recover_index].data(), MFSBLOCKSIZE, i * MFSBLOCKSIZE);
			recover_index++;
		}
	}
	return 0;
}