#include "common/platform.h"
#include "common/disk_info.h"

void HddStatistics::add(const HddStatistics& other) {
	rbytes += other.rbytes;
	wbytes += other.wbytes;
	usecreadsum += other.usecreadsum;
	usecwritesum += other.usecwritesum;
	usecfsyncsum += other.usecfsyncsum;
	rops += other.rops;
	wops += other.wops;
	fsyncops += other.fsyncops;
	if (other.usecreadmax > usecreadmax) {
		usecreadmax = other.usecreadmax;
	}
	if (other.usecwritemax > usecwritemax) {
		usecwritemax = other.usecwritemax;
	}
	if (other.usecfsyncmax > usecfsyncmax) {
		usecfsyncmax = other.usecfsyncmax;
	}
}
