package geospat1.operation1;

class Point{
    private double x;
    private double y;

    public double getX() {
        return x;
    }

    public void setX(int x) {
        this.x = x;
    }

    public double getY() {
        return y;
    }

    public void setY(int y) {
        this.y = y;
    }

    public Point(double num, double num2) {
        this.x = num;
        this.y = num2;
    }
    
    public String toString()
    {  return "(" + x + ", " + y + ")";  }


    public Point() {
    }
}
