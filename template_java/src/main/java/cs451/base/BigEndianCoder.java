package cs451.base;

public final class BigEndianCoder {
    // returns a byte array of length 4
    public static byte[] encodeInt(int value) {
        return new byte[] {
            (byte) ( value         & 0xFF),
            (byte) ((value >>>  8) & 0xFF),
            (byte) ((value >>> 16) & 0xFF),
            (byte) ((value >>> 24) & 0xFF)
        };
    }

    // encode directly into a large array
    public static void encodeInt(int value, byte[] array, int startPosition) {
        array[startPosition] = (byte) (value & 0xFF);
        array[startPosition + 1] = (byte) ((value >>> 8) & 0xFF);
        array[startPosition + 2] = (byte) ((value >>> 16) & 0xFF);
        array[startPosition + 3] = (byte) ((value >>> 24) & 0xFF);
    }

    // inverse operation of encodeInt
    public static int decodeInt(byte[] bytes) {
        if (bytes.length != 4) {
            throw new IllegalStateException("wrong length, expected 4 but got " + bytes.length);
        }
        return  (bytes[0] + 256) % 256
            + (((bytes[1] + 256) % 256) <<  8)
            + (((bytes[2] + 256) % 256) << 16)
            + (((bytes[3] + 256) % 256) << 24);
    }

    // decode directly from a large array
    public static int decodeInt(byte[] bytes, int startPosition) {
        return  (bytes[startPosition] + 256) % 256
                + (((bytes[startPosition + 1] + 256) % 256) <<  8)
                + (((bytes[startPosition + 2] + 256) % 256) << 16)
                + (((bytes[startPosition + 3] + 256) % 256) << 24);
    }
}
