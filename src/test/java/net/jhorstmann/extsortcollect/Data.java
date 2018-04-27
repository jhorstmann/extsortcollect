package net.jhorstmann.extsortcollect;

class Data {
    private final int id;
    private final String key;
    private final String payload;

    Data(int id, String key, String payload) {
        this.id = id;
        this.key = key;
        this.payload = payload;
    }

    int getId() {
        return id;
    }

    String getKey() {
        return key;
    }

    public String getPayload() {
        return payload;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Data data = (Data) o;

        if (id != data.id) return false;
        return key.equals(data.key);
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public String toString() {
        return key;
    }
}
