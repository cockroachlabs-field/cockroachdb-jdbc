package io.cockroachdb.jdbc.it.util;

public abstract class TextUtils {
    private TextUtils() {
    }

    public static String shrug() {
        return "¯\\_(ツ)_/¯";
    }

    public static String flipTableGently() {
        return "(╯°□°)╯︵ ┻━┻";
    }

    public static String flipTableVeryRoughly() {
        return "(ノಠ益ಠ)ノ彡┻━┻";
    }

    public static String progressBar(int total, int current) {
        double p = (current + 0.0) / (Math.max(1, total) + 0.0);
        int ticks = Math.max(0, (int) (30 * p) - 1);
        return String.format(
                "\u001B[33m%3d/%-3d %4.1f%%[%-30s]\u001B[0m",
                current,
                total,
                p * 100.0,
                new String(new char[ticks]).replace('\0', '#') + ">");
    }

    public static String successRate(String prefix, int success, int failures) {
        return String.format("%s \u001B[33m(Success: %d| Failures: %d| Success Rate: %.2f%%)\u001B[0m",
                prefix,
                success,
                failures,
                100 - (failures / (double) (Math.max(1, success + failures))) * 100.0);
    }
}
