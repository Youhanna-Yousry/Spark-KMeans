import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class KMeans {
    private static final Logger log = Logger.getLogger(KMeans.class);

    public static void main(String[] args) {

        if (args.length != 4) {
            log.info("Usage: KMeans <input file> <output file> <k> <max iterations>");
            System.exit(1);
        }

        String inputFile = args[0];
        String outputFile = args[1];
        int k = Integer.parseInt(args[2]);
        int maxIterations = Integer.parseInt(args[3]);

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("KMeans");

        long startTime = System.currentTimeMillis();

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            // Read input file
            JavaRDD<String> input = sc.textFile(inputFile);

            // Map each line to point
            JavaRDD<Point> points = input.map(Point::new).cache();

            // Select random points
            List<Point> selectedPoints = points.takeSample(false, k);

            // Assign index to each centroid
            List<Tuple2<Integer, Point>> centroidsTuples = new ArrayList<>();

            for (int i = 0; i < k; i++) {
                centroidsTuples.add(new Tuple2<>(i, selectedPoints.get(i)));
            }

            JavaPairRDD<Integer, Point> centroids = sc.parallelizePairs(centroidsTuples).sortByKey();

            boolean converged = false;
            int iteration = 0;

            while (!converged) {
                if (iteration >= maxIterations) {
                    log.info("Max iterations reached");
                    break;
                }

                // Copying centroids to make it effectively final
                List<Point> tempCentroids = centroids.values().collect();

                // Assign points to clusters
                JavaPairRDD<Integer, Point> assignedPoints = points.mapToPair(x -> x.assignPoint(tempCentroids));

                // Aggregate points in each cluster
                JavaPairRDD<Integer, Point> aggregatedClusterPoints = assignedPoints.reduceByKey(Point::add);

                // Scale the centroids by dividing by the count
                JavaPairRDD<Integer, Point> newCentroids = aggregatedClusterPoints
                        .mapValues(Point::scale)
                        .sortByKey();

                // Check for convergence
                converged = newCentroids.join(centroids)
                        .values()
                        .map(tuple -> tuple._1().computeDistance(tuple._2()) <= 1e-6)
                        .reduce((a, b) -> a && b);

                // Update centroids
                centroids = newCentroids;
                iteration++;
            }

            // Write output to file
            JavaRDD<String> output = centroids.mapValues(Point::toString).values();
            output.saveAsTextFile(outputFile);

            // Log centroids
            log.info("Centroids:");
            int idx = 0;
            for (Point centroid : centroids.values().collect()) {
                log.info("Centroid " + idx++ + ": " + centroid);
            }

            log.info("Converged in " + iteration + " iterations");
            log.info("Time taken: " + (double) (System.currentTimeMillis() - startTime) / 1000 + " seconds");
        }
    }
}
