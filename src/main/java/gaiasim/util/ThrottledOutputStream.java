package gaiasim.util;

import com.revinate.guava.util.concurrent.RateLimiter;

import java.io.IOException;
import java.io.OutputStream;

public final class ThrottledOutputStream extends OutputStream {
    private final OutputStream out;
    private final RateLimiter rateLimiter;

    public ThrottledOutputStream(OutputStream out, double bytesPerSecond) {
        this.out = out;
        this.rateLimiter = RateLimiter.create(bytesPerSecond);
    }

    public void setRate(double bytesPerSecond) {
        rateLimiter.setRate(bytesPerSecond);
    }

    @Override
    public void write(int b) throws IOException {
        rateLimiter.acquire();
        out.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        rateLimiter.acquire(b.length);
        out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        rateLimiter.acquire(len);
        out.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void close() throws IOException {
        out.close();
    }
}