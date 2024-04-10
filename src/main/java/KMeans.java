import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KMeans {

    private static boolean hasConverged(List<Point> centroids, List<Point> newCentroids, float threshold) {
        for (int i = 0; i < centroids.size(); i++) {
            if (centroids.get(i).computeDistance(newCentroids.get(i)) > threshold) {
                return false;
            }
        }
        return true;
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            System.err.println("Usage: KMeans <input file> <output file> <k> <max iterations>");
            System.exit(1);
        }

        String inputFile = args[0];
        String outputFile = args[1];
        int k = Integer.parseInt(args[2]);
        int maxIterations = Integer.parseInt(args[3]);

        SparkConf conf = new SparkConf().setMaster("local").setAppName("KMeans");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            // Read input file
            JavaRDD<String> input = sc.textFile(inputFile);

            // Split up into lines
            JavaRDD<String> lines = input.flatMap(x -> Arrays.asList(x.split("\n")).iterator());

            // Map each line to point
            JavaRDD<Point> points = lines.map(Point::new);

            // Select random centroids
            List<Point> centroids = Randomizer.chooseCentroids(points.collect(), k);

            boolean converged = false;
            int iteration = 0;

            while (!converged) {
                if (iteration >= maxIterations) {
                    System.out.println("Max iterations reached");
                    break;
                }

                // Copying centroids to make it effectively final
                List<Point> tempCentroids = new ArrayList<>(centroids);

                // Assign points to clusters
                JavaPairRDD<Integer, Point> assignedPoints = points.mapToPair(x -> x.assignPoint(tempCentroids));

                // Compute new centroids
                JavaPairRDD<Integer, Point> clusterPoints = assignedPoints.reduceByKey(Point::add);

                // Scale the centroids by dividing by the count
                JavaRDD<Point> newCentroids = clusterPoints.map(x -> x._2.scale());

                converged = hasConverged(centroids, newCentroids.collect(), 1e-6f);

                // update centroids
                centroids = newCentroids.collect();
                iteration++;
            }

            // Write output to file
            JavaRDD<String> output = sc.parallelize(centroids).map(Point::toString);
            output.saveAsTextFile(outputFile);
        }
    }
}
