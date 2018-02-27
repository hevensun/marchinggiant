package com.xiaomi.ad.keyword.marchinggiant.java;

import java.io.Serializable;
import java.util.*;

/**
 * Created by cailiming on 17-5-23.
 */
public class Node implements Serializable {
    private Map<Character, Node> map;
    private List<String> emits; //输出
    private Node failure; //失败中转
    private boolean isRoot = false;//是否为根结点

    public Node(){
        map = new HashMap<>();
        emits = new ArrayList<>();
    }

    public Node(boolean isRoot) {
        this();
        this.isRoot = isRoot;
    }

    public Node insert(Character character) {
        Node node = this.map.get(character);
        if (node == null) {
            node = new Node();
            map.put(character, node);
        }
        return node;
    }

    public void addEmit(String keyword) {
        emits.add(keyword);
    }

    public void addEmit(Collection<String> keywords) {
        emits.addAll(keywords);
    }

    /**
     * success跳转
     * @param character
     * @return
     */
    public Node find(Character character) {
        return map.get(character);
    }

    /**
     * 跳转到下一个状态
     * @param transition 接受字符
     * @return 跳转结果
     */
    public Node nextState(Character transition) {
        Node state = this.find(transition);  // 先按success跳转
        if (state != null) {
            return state;
        }
        //如果跳转到根结点还是失败，则返回根结点
        if (this.isRoot) {
            return this;
        }
        // 跳转失败的话，按failure跳转
        return this.failure.nextState(transition);
    }

    public Collection<Node> children() {
        return this.map.values();
    }

    public void setFailure(Node node) {
        failure = node;
    }

    public Node getFailure() {
        return failure;
    }

    public Set<Character> getTransitions() {
        return map.keySet();
    }

    public Collection<String> emit() {
        return this.emits == null ? Collections.<String>emptyList() : this.emits;
    }
}
