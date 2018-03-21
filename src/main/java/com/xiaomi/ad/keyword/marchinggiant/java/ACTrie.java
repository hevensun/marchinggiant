package com.xiaomi.ad.keyword.marchinggiant.java;

import java.io.Serializable;
import java.util.*;

/**
 */
public class ACTrie implements Serializable {
    private boolean failureStatesConstructed = false; //是否建立了failure表
    private Node root; //根结点

    private static class INSTANCE_HELPER {
        static final ACTrie INSTANCE = new ACTrie();
    }

    private ACTrie() {
        this.root = new Node(true);
    }

    public static ACTrie getInstance() {
        return INSTANCE_HELPER.INSTANCE;
    }

    /**
     * 添加一个模式串
     * @param keyword
     */
    public void addKeyword(String keyword) {
        if (keyword == null || keyword.length() == 0) {
            return;
        }
        Node currentState = this.root;
        for (Character character : keyword.toCharArray()) {
            currentState = currentState.insert(character);
        }
        currentState.addEmit(keyword);
    }

    /**
     * 模式匹配
     *
     * @param text 待匹配的文本
     * @return 匹配到的模式串
     */
    public Collection<Emit> parseText(String text) {
        checkForConstructedFailureStates();
        Node currentState = this.root;
        List<Emit> collectedEmits = new ArrayList<>();
        for (int position = 0; position < text.length(); position++) {
            Character character = text.charAt(position);
            currentState = currentState.nextState(character);
            Collection<String> emits = currentState.emit();
            if (emits == null || emits.isEmpty()) {
                continue;
            }
            for (String emit : emits) {
                collectedEmits.add(new Emit(position - emit.length() + 1, position, emit));
            }
        }
        return collectedEmits;
    }

    public Collection<String> parseTextAndMerge(String text) {
        Collection<Emit> parsed = parseText(text);
        List<String> ans = new ArrayList<>();
        if (parsed.isEmpty()) {
            return ans;
        }
        Emit last = null;
        List<Emit> parsedList = new ArrayList<>(parsed);
        for (int i = parsedList.size() - 1; i >= 0; i--) {
            Emit cur = parsedList.get(i);
            if (last == null) {
                last = cur;
            } else {
                if (last.getStart() <= cur.getStart() && last.getEnd() >= cur.getEnd()) {
                    continue;
                } else if (cur.getStart() <= last.getStart() && cur.getEnd() >= last.getEnd()) {
                    last = cur;
                } else {
                    ans.add(last.getKeyword());
                    last = cur;
                }
            }
        }
        if (last != null) {
            ans.add(last.getKeyword());
        }

        Collections.reverse(ans);
        return ans;
    }

    /**
     * 检查是否建立了failure表
     */
    private void checkForConstructedFailureStates() {
        if (!this.failureStatesConstructed) {
            constructFailureStates();
        }
    }

    /**
     * 建立failure表
     */
    private void constructFailureStates() {
        Queue<Node> queue = new LinkedList<>();

        // 第一步，将深度为1的节点的failure设为根节点
        //特殊处理：第二层要特殊处理，将这层中的节点的失败路径直接指向父节点(也就是根节点)。
        for (Node depthOneState : this.root.children()) {
            depthOneState.setFailure(this.root);
            queue.add(depthOneState);
        }
        this.failureStatesConstructed = true;

        // 第二步，为深度 > 1 的节点建立failure表，这是一个bfs 广度优先遍历
        /**
         * 构造失败指针的过程概括起来就一句话：设这个节点上的字母为C，沿着他父亲的失败指针走，直到走到一个节点，他的儿子中也有字母为C的节点。
         * 然后把当前节点的失败指针指向那个字母也为C的儿子。如果一直走到了root都没找到，那就把失败指针指向root。
         * 使用广度优先搜索BFS，层次遍历节点来处理，每一个节点的失败路径。　　
         */
        while (!queue.isEmpty()) {
            Node parentNode = queue.poll();

            for (Character transition : parentNode.getTransitions()) {
                Node childNode = parentNode.find(transition);
                queue.add(childNode);

                Node failNode = parentNode.getFailure().nextState(transition);

                childNode.setFailure(failNode);
                childNode.addEmit(failNode.emit());
            }
        }
    }

    public static void main(String[] args) {
        ACTrie trie = new ACTrie();
        trie.addKeyword("ip");
        trie.addKeyword("ph");
        trie.addKeyword("ho");
        trie.addKeyword("iphon");
        trie.addKeyword("hon");
        trie.addKeyword("on");
        trie.addKeyword("iphone");
        trie.addKeyword("phone");
        trie.addKeyword("one");
        trie.addKeyword("ne");
        trie.addKeyword("iphone4");
        trie.addKeyword("e4");
        Collection<Emit> emits = trie.parseText("iphone4");
        for (Emit emit : emits) {
            System.out.println(emit.getStart() + " " + emit.getEnd() + "\t" + emit.getKeyword());
        }

        Collection<String> ans = trie.parseTextAndMerge("iphone4");
        for (String cur : ans) {
            System.out.println(cur);
        }
    }
}
