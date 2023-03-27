package redis;

import com.google.common.base.Charsets;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.ReplayingDecoder;
import redis.netty4.Command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static redis.netty4.RedisReplyDecoder.readLong;

/**
 * Decode commands.
 */
public class RedisCommandDecoder extends ReplayingDecoder<RedisCommandDecoder.InputState> {
    private int length;
    private List<byte[]> params;

    static class InputState {
        public int index;
    }

    @Override
    public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        InputState state = this.state();
        if (state == null) {
            length = readParamsLen(in);
            this.params = new ArrayList<>(length);
            state = new InputState();
            this.checkpoint(state);
        }
        for (int i = state.index; i < length; i++) {
            byte[] param = readParam_(in);
            this.params.add(param);
            state.index = state.index + 1;
            this.checkpoint(state);
        }
        out.add(new Command(params.toArray()));
        this.checkpoint(null);
    }

    private final static int CR = '\r';
    private final static int LF = '\n';
    private final static int DOLLAR = '$';
    private final static int ASTERISK = '*';

    private int readParamsLen(ByteBuf in) {
        int c = in.readByte();
        if (c != ASTERISK) {
            throw new DecoderException("expect character *");
        }
        int len = readLen(in, 3); // max 999 params
        if (len == 0) {
            throw new DecoderException("expect non-zero params");
        }
        return len;
    }

    private String readParam(ByteBuf in) {
        int len = readStrLen(in);
        return readStr(in, len);
    }

    private byte[] readParam_(ByteBuf in) {
        int len = readStrLen(in);
        return readByte(in, len);
    }

    private byte[] readByte(ByteBuf in, int len) {
        if (len == 0) {
            return null;
        }
        byte[] cs = new byte[len];
        in.readBytes(cs);
        skipCrlf(in);
        return cs;
    }

    private String readStr(ByteBuf in, int len) {
        if (len == 0) {
            return "";
        }
        byte[] cs = new byte[len];
        in.readBytes(cs);
        skipCrlf(in);
        return new String(cs, Charsets.UTF_8);
    }

    private int readStrLen(ByteBuf in) {
        int c = in.readByte();
        if (c != DOLLAR) {
            throw new DecoderException("expect character $");
        }
        return readLen(in, 6); // string maxlen 999999
    }

    private int readLen(ByteBuf in, int maxBytes) {
        byte[] digits = new byte[maxBytes]; // max 999个参数
        int len = 0;
        while (true) {
            byte d = in.getByte(in.readerIndex());
            if (!Character.isDigit(d)) {
                break;
            }
            in.readByte();
            digits[len] = d;
            len++;
            if (len > maxBytes) {
                throw new DecoderException("params length too large");
            }
        }
        skipCrlf(in);
        if (len == 0) {
            throw new DecoderException("expect digit");
        }
        return Integer.parseInt(new String(digits, 0, len));
    }

    private void skipCrlf(ByteBuf in) {
        int c = in.readByte();
        if (c == CR) {
            c = in.readByte();
            if (c == LF) {
                return;
            }
        }
        throw new DecoderException("expect cr ln");
    }


}
