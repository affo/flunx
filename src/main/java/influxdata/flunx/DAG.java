package influxdata.flunx;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;

public class DAG {
    Set<String> maybeRoots = new HashSet<>();
    List<Node> roots = new ArrayList<>();
    Map<String, Node> nodes = new HashMap<>();

    public void addNode(String parent, String child) {
        maybeRoots.remove(child);
        Node p = nodes.get(parent);
        Node c = nodes.get(child);

        // create nodes if they don't exist
        if (p == null) {
            p = new Node(parent);
            nodes.put(parent, p);
            maybeRoots.add(parent);
        }

        if (c == null) {
            c = new Node(child);
            nodes.put(child, c);
        }


        p.addNode(c);
    }

    public void walk(Consumer<Node> fn) {
        walk(new Visitor() {
            @Override
            public Visitor Visit(Node node) {
                fn.accept(node);
                return this;
            }
        });
    }

    public void walk(Visitor v) {
        // the DAG is complete, let's fill the roots
        if (roots.isEmpty()) {
            for (String n : maybeRoots) {
                roots.add(nodes.get(n));
            }
        }

        for (Node r : roots) {
            walk(r, v);
        }
    }

    private void walk(Node node, Visitor v) {
        Visitor vv = v.Visit(node);

        if (vv != null) {
            for (Node r : node.children) {
                walk(r, vv);
            }
        }
    }

    public class Node {
        String id;
        JsonNode spec;
        List<Node> children;

        public Node(String id) {
            this.id = id;
            this.children = new ArrayList<>();
        }

        public void addNode(Node node) {
            this.children.add(node);
        }

        public void setSpec(JsonNode spec) {
            this.spec = spec;
        }

        @Override
        public String toString() {
            return id;
        }
    }

    public static DAG fromSPEC(String path) throws IOException {
        ObjectMapper des = new ObjectMapper();
        JsonNode spec = des.readTree(new File(path)).get("spec");
        JsonNode edges = spec.get("edges");
        JsonNode ops = spec.get("operations");
        DAG dag = new DAG();

        for (JsonNode edge : edges) {
            String parent = edge.get("parent").asText();
            String child = edge.get("child").asText();
            dag.addNode(parent, child);
        }

        dag.walk((node) -> {
            for (JsonNode op : ops) {
                if (op.get("id").asText().equals(node.id)) {
                    node.setSpec(op);
                }
            }
        });
        return dag;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        this.walk((node) -> {
            sb.append(node.id);
            sb.append(": ");
            sb.append(node.spec);
            sb.append(" -> ");
            sb.append(node.children.toString());
            sb.append("\n");
        });
        return sb.toString();
    }

    interface Visitor {
        Visitor Visit(Node node);
    }
}
