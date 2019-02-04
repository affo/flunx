package influxdata.flunx;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

public class DAG {
    private Set<String> notRoots = new HashSet<>();
    private Set<String> notSinks = new HashSet<>();
    private List<Node> roots = new ArrayList<>();
    private List<Node> sinks = new ArrayList<>();
    private Map<String, Node> nodes = new HashMap<>();

    public void addNode(String parent, String child) {
        Node p = null, c = null;

        if (parent != null && child != null) {
            notSinks.add(parent);
            notRoots.add(child);
        }

        if (parent != null) {
            p = nodes.get(parent);
            if (p == null) {
                p = new Node(parent);
                nodes.put(parent, p);
            }
        }

        if (child != null) {
            c = nodes.get(child);
            if (c == null) {
                c = new Node(child);
                nodes.put(child, c);
            }
        }

        if (p != null && c != null) {
            p.addChild(c);
            c.addParent(p);
        }
    }

    private void calculateRootsAndSinks() {
        roots.clear();
        for (String n : nodes.keySet()) {
            if (!notRoots.contains(n)) {
                roots.add(nodes.get(n));
            }
        }

        sinks.clear();
        for (String n : nodes.keySet()) {
            if (!notSinks.contains(n)) {
                sinks.add(nodes.get(n));
            }
        }
    }

    public List<Stream> getOutputStreams() {
        return sinks.stream()
                .map(p -> p.outputStream)
                .collect(Collectors.toList());
    }

    public void walk(VisitFn<Node> fn) throws Exception {
        walk(new Visitor() {
            @Override
            public Visitor Visit(Node node) throws Exception {
                fn.apply(node);
                return this;
            }
        });
    }

    public void walk(Visitor v) throws Exception {
        for (Node r : roots) {
            walk(r, v);
        }
    }

    // TODO(affo) need a topological walk
    private void walk(Node node, Visitor v) throws Exception {
        Visitor vv = v.Visit(node);

        if (vv != null) {
            for (Node r : node.children) {
                walk(r, vv);
            }
        }
    }

    public static class Node {
        JsonNode opSpec;
        final String id;
        private String kind;
        private List<Node> children;
        private List<Node> parents;

        private Stream outputStream;

        public Node(String id) {
            this.id = id;
            this.children = new ArrayList<>();
            this.parents = new ArrayList<>();
        }

        public void addChild(Node node) {
            this.children.add(node);
        }

        public void addParent(Node node) {
            this.parents.add(node);
        }

        public void setOpSpec(JsonNode opSpec) {
            setKind(opSpec.get("kind").asText());
            this.opSpec = opSpec;
        }

        public JsonNode getSpec() {
            return this.opSpec.get("spec");
        }

        public String getKind() {
            return this.kind;
        }

        public void setKind(String kind) {
            this.kind = kind;
        }

        public void setOutputStream(Object outputStream) {
            this.outputStream = new Stream(outputStream);
        }

        public List<Stream> getInputStreams() {
            return parents.stream()
                    .map(p -> p.outputStream)
                    .collect(Collectors.toList());
        }

        // Expects that there is only one input stream, crashes otherwise.
        public Stream getInputStream() {
            if (parents.size() > 1 || parents.isEmpty()) {
                throw new IllegalStateException(getKind() + ": expected one input stream, got" + parents.size());
            }

            return getInputStreams().get(0);
        }

        public void chainTransformation() throws Exception {
            Transformation t = Transformations.get(this.getKind());
            if (t == null) {
                System.err.println("no match found for " + getKind() + ", by-passing it");
                // directly chaining the input to the output with a NOP
                this.setOutputStream(getInputStream().toVanilla());
                return;
            }

            t.chain(FluxJob.env, this);
        }

        @Override
        public String toString() {
            return id;
        }
    }

    public static class Stream {
        private Object flinkStream;

        public Stream(Object flinkStream) {
            this.flinkStream = flinkStream;
        }

        public boolean isKeyed() {
            return this.flinkStream instanceof KeyedStream;
        }

        // Note that a KeyedStream is also a Vanilla stream
        public boolean isVanilla() {
            return this.flinkStream instanceof DataStream;
        }


        public boolean isWindowed() {
            return this.flinkStream instanceof WindowedStream;
        }

        public KeyedStream<Row, String> toKeyed() {
            return (KeyedStream<Row, String>) this.flinkStream;
        }

        public WindowedStream<Row, String, TimeWindow> toWindowed() {
            return (WindowedStream<Row, String, TimeWindow>) this.flinkStream;
        }

        public DataStream<Row> toVanilla() {
            return (DataStream<Row>) this.flinkStream;
        }
    }

    public static DAG fromSPEC(String path) throws Exception {
        ObjectMapper des = new ObjectMapper();
        JsonNode spec = des.readTree(new File(path));
        JsonNode edges = spec.get("edges");
        JsonNode ops = spec.get("operations");
        DAG dag = new DAG();

        if (edges.asText().equals("null")) {
            // there is no edge, everything inside ops is a source and a sink
            for (JsonNode op : ops) {
                String nodeId = op.get("id").asText();
                dag.addNode(nodeId, null);
                dag.addNode(null, nodeId);
            }
        } else {
            for (JsonNode edge : edges) {
                String parent = edge.get("parent").asText();
                String child = edge.get("child").asText();
                dag.addNode(parent, child);
            }
        }
        dag.calculateRootsAndSinks();

        dag.walk((node) -> {
            for (JsonNode op : ops) {
                if (op.get("id").asText().equals(node.id)) {
                    node.setOpSpec(op);
                    break;
                }
            }
        });

        // add generated yields
        int i = 0;
        for (Node sink : dag.sinks) {
            if (!sink.getKind().startsWith("to")) {
                // add yield
                DAG.Node yield = new DAG.Node("yield" + i);
                yield.setKind("yield");
                sink.addChild(yield);
                yield.addParent(sink);
                i++;
            }
        }

        System.err.println(dag);

        return dag;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        try {
            this.walk((node) -> {
                sb.append(node.id);
                sb.append(": ");
                sb.append(node.opSpec);
                sb.append(" -> ");
                sb.append(node.children.toString());
                sb.append("\n");
            });
        } catch (Exception e) {
            sb.append(e.getMessage());
        }
        return sb.toString();
    }

    interface Visitor {
        Visitor Visit(Node node) throws Exception;
    }

    @FunctionalInterface
    interface VisitFn<T> {
        void apply(T v) throws Exception;
    }
}
