package fleetdb;

import java.io.InputStream;
import java.io.FilterInputStream;
import java.io.IOException;

public class LimitInputStream extends FilterInputStream {
    private volatile long bytesLeft;

    public LimitInputStream(InputStream in, long bytesAllowed) {
      super(in);
      this.bytesLeft = bytesAllowed;
    }

    public int read() throws IOException {
      if (this.bytesLeft > 0) {
        this.bytesLeft--;
        return super.read();
      } else {
        return -1;
      }
    }

    public int read(byte[] b) throws IOException {
      if (this.bytesLeft > 0) {
        int read;
        if (this.bytesLeft > Integer.MAX_VALUE) {
          read = super.read(b);
        } else {
          read = super.read(b, 0, (int)this.bytesLeft);
        }
        this.bytesLeft -= read;
        return read;
      } else {
        return -1;
      }
    }

    public int read(byte[] b, int off, int len) throws IOException {
      if (this.bytesLeft > 0) {
        int read;
        if (this.bytesLeft > Integer.MAX_VALUE) {
          read = super.read(b, off, len);
        } else {
          read = super.read(b, off, Math.min((int)this.bytesLeft, len));
        }
        this.bytesLeft -= read;
        return read;
      } else {
        return -1;
      }
    }

    public long skip(long n) throws IOException {
      throw new UnsupportedOperationException();
    }

    public void mark(int readlimit) {
        throw new UnsupportedOperationException();
    }

    public void reset() throws IOException {
        throw new UnsupportedOperationException();
    }

    public boolean markSupported() {
        return false;
    }
}