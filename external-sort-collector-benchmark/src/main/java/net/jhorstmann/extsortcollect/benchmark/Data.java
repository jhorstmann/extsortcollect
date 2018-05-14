package net.jhorstmann.extsortcollect.benchmark;

class Data {
    private final int id;
    private final String payload;

    Data(int id, String payload) {
        this.id = id;
        this.payload = payload;
    }

    int getId() {
        return id;
    }

    String getPayload() {
        return payload;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Data data = (Data) o;

        if (id != data.id) return false;
        return payload.equals(data.payload);
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public String toString() {
        return payload;
    }
}
