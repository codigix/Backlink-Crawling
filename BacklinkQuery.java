// java -Xmx4g -cp <cc-webgraph jar> BacklinkQuery.java <graph-prefix> <domain>
// Prints one referring domain per line, then "COUNT n". Labels in the graph are reversed (co.codigix).
import org.commoncrawl.webgraph.explore.Graph;

public class BacklinkQuery {
    static String rev(String d) {
        String[] p = d.split("\\.");
        StringBuilder b = new StringBuilder();
        for (int i = p.length - 1; i >= 0; i--) b.append(p[i]).append(i > 0 ? "." : "");
        return b.toString();
    }

    public static void main(String[] a) throws Exception {
        Graph g = new Graph(a[0]);
        String label = rev(a[1]);
        long id = g.vertexLabelToId(label);
        if (id < 0) { System.out.println("NOT_IN_GRAPH"); return; }
        long[] n = {0};
        g.predecessorStream(label).forEach(s -> { System.out.println(rev(s)); n[0]++; });
        System.out.println("COUNT " + n[0]);
    }
}
