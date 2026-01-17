package com.bullit.data.adapter.driven.jpa;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;

@Entity
@Table(name = "foo")
public class FooEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "foo_seq")
    @SequenceGenerator(name = "foo_seq", sequenceName = "foo_seq", allocationSize = 1000)
    @Column(name = "foo_id", nullable = false)
    private Long fooId;

    @Column(name = "foo1", nullable = false, length = 255)
    private String foo1;

    @Column(name = "foo2", nullable = false, length = 255)
    private String foo2;

    @Column(name = "foo3", nullable = false, length = 255)
    private String foo3;

    protected FooEntity() {
    }

    public FooEntity(String foo1, String foo2, String foo3) {
        this.foo1 = foo1;
        this.foo2 = foo2;
        this.foo3 = foo3;
    }

    public Long getFooId() {
        return fooId;
    }

    public String getFoo1() {
        return foo1;
    }

    public String getFoo2() {
        return foo2;
    }

    public String getFoo3() {
        return foo3;
    }
}
