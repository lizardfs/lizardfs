#include <poll.h>
#include <inttypes.h>

void main_destructregister (void (*)(void)) {
}

void main_canexitregister (int (*)(void)) {
}

void main_wantexitregister (void (*)(void)) {
}

void main_reloadregister (void (*)(void)) {
}

void main_pollregister (void (*)(struct pollfd *,uint32_t *), void (*)(struct pollfd *)) {
}

void main_eachloopregister (void (*)(void)) {
}

void* main_timeregister (int, uint32_t, uint32_t, void (*)(void)) {
	return 0;
}

int main_timechange(void*, int, uint32_t, uint32_t) {
	return 0;
}
