import java.io.Serializable;

/**
 * Created by ashvinikumar on 31/7/17.
 */
public class Employee implements Serializable {
    private int sid;
    private String name;
    private String address;

    public int getSid() {
        return sid;
    }

    public void setSid(int sid) {
        this.sid = sid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }


}
