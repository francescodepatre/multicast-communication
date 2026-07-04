import java.io.*;

public class Message implements Serializable {

    public enum Type { DATA, RESEND, LOSS_NOTICE }

    public final Type type;

    public final int senderId;

    public final int msgId;

    public Message(Type type, int senderId, int msgId) {
        this.type = type;
        this.senderId = senderId;
        this.msgId = msgId;
    }

    public byte[] serialize() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(this);
        }
        return bos.toByteArray();
    }

    public static Message deserialize(byte[] data, int length) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(data, 0, length);
        try (ObjectInputStream ois = new ObjectInputStream(bis)) {
            return (Message) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    @Override
    public String toString() {
        return "Message{type=" + type + ", senderId=" + senderId + ", msgId=" + msgId + '}';
    }
}