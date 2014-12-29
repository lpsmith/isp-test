#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <stdint.h>
#include <sys/select.h>
#include <time.h>
#include <endian.h>

#define BUFFER_SIZE 32756
#define BUFFER_RESERVED 12
#define WRITE_THRESHOLD 1024

static char buffer[BUFFER_RESERVED + BUFFER_SIZE];
static char * rp;
static ssize_t left;

static int
write_buffer()
{
  ssize_t len = BUFFER_RESERVED + BUFFER_SIZE - left;
  int errsv;
  struct timespec tp;
  if (clock_gettime(CLOCK_REALTIME, &tp)) {
    int errsv = errno;
    fprintf(stderr, "clock_gettime: %s\n", strerror(errsv));
    errno = errsv;
    return -1;
  }
  // Convert to PostgreSQL time format
  int64_t ts = ((int64_t)tp.tv_sec + 946684800) * 1000000
             + ((int64_t)tp.tv_nsec / 1000);
  *((uint64_t*)buffer) = htobe64((uint64_t)ts);
  *((uint32_t*)(buffer + sizeof(int64_t))) = htobe32((uint32_t)len);
  char * wp = buffer;
  while (len > 0) {
    ssize_t retval = write(STDOUT_FILENO, wp, len);
    if (retval < 0) {
      switch(errno) {
      case EAGAIN:
      case EINTR:
        break;
      default:
        errsv = errno;
        fprintf(stderr, "write: %s\n", strerror(errsv));
        errno = errsv;
        return -1;
      }
    } else {
      wp  += retval;
      len -= retval;
    }
  }
  rp = buffer + BUFFER_RESERVED;
  left = BUFFER_SIZE;
  return 0;
}


int
main (void)
{
  int errsv;
  fd_set rfds;
  FD_ZERO(&rfds);
  FD_SET(0, &rfds);

  int flags = fcntl(STDIN_FILENO, F_GETFL, 0);
  fcntl(STDIN_FILENO, F_SETFL, flags | O_NONBLOCK);

  rp   = buffer + BUFFER_RESERVED;
  left = BUFFER_SIZE;

  while (1) {
    ssize_t len = read(STDIN_FILENO, rp, left);
    if (len < 0) {
      switch(errno) {
      case EAGAIN:
        if (write_buffer()) return errno;
        if (select(1, &rfds, NULL, NULL, NULL) == -1) {
          switch (errno) {
          case EINTR:
            break;
          default:
            errsv = errno;
            fprintf(stderr, "select: %s\n", strerror(errsv));
            errno = errsv;
            return errno;
          }
        }
        break;
      case EINTR:
        break;
      default:
        errsv = errno;
        fprintf(stderr, "read: %s\n", strerror(errsv));
        errno = errsv;
        return errno;
      }
    } else if (len == 0) {
      return 0;
    } else {
      rp += len;
      left -= len;
      if (left < WRITE_THRESHOLD) {
        if (write_buffer()) return errno;
      }
    }
  }
}
