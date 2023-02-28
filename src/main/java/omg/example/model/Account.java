package omg.example.model;

public class Account {
    String name;
    int point;
    int transactionCount;
    public Account(String name) {
        this.name = name;
        this.point = 0;
        transactionCount = 0;
    }
    public String name(){return name;}
    public int point(){return point;}
    public void changePoint(int point){
        this.point += point;
        transactionCount += 1;
    }

    @Override
    public String toString() {
        return "{" +
                "name='" + name + '\'' +
                ", point=" + point +
                ", transactionCount=" + transactionCount +
                '}';
    }
}
