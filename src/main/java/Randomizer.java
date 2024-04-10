import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Randomizer {
    public static List<Point> chooseCentroids(List<Point> points, int k) {
        List<Point> centroids = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < k; i++) {
            int index = random.nextInt(points.size());
            centroids.add(points.get(index));
        }
        return centroids;
    }
}
