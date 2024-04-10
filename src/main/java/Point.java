import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Point implements java.io.Serializable {
    private final List<Float> coordinates;
    private final int count;

    public Point(List<Float> coordinates, int count) {
        this.coordinates = coordinates;
        this.count = count;
    }

    public Point(String line) {
        String[] parts = line.split(",");
        List<Float> vector = new ArrayList<>();
        for (String part : parts) {
            vector.add(Float.parseFloat(part));
        }
        this.coordinates = vector;
        this.count = 1;
    }

    public List<Float> getCoordinates() {
        return coordinates;
    }

    public float computeDistance(Point other) {
        float distance = 0;
        for (int i = 0; i < coordinates.size(); i++) {
            distance += (float) Math.pow(coordinates.get(i) - other.getCoordinates().get(i), 2);
        }
        return (float) Math.sqrt(distance);
    }

    public Tuple2<Integer, Point> assignPoint(List<Point> centroids) {
        float minDistance = Float.MAX_VALUE;
        int cluster = 0;
        for (int i = 0; i < centroids.size(); i++) {
            float distance = computeDistance(centroids.get(i));
            if (distance < minDistance) {
                minDistance = distance;
                cluster = i;
            }
        }
        return new Tuple2<>(cluster, this);
    }

    public Point add(Point other) {
        List<Float> newCoordinates = new ArrayList<>();
        for (int i = 0; i < coordinates.size(); i++) {
            newCoordinates.add(coordinates.get(i) + other.getCoordinates().get(i));
        }
        return new Point(newCoordinates, this.count + other.count);
    }

    public Point scale() {
        List<Float> newCoordinates = new ArrayList<>();
        for (Float coordinate : coordinates) {
            newCoordinates.add(coordinate / count);
        }
        return new Point(newCoordinates, 1);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < coordinates.size(); i++) {
            sb.append(coordinates.get(i));
            if (i < coordinates.size() - 1) {
                sb.append(",");
            }
        }
        return sb.toString();
    }
}
