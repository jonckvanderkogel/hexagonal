package com.bullit.data.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.util.UUID;

@Entity
@Table(name = "authors")
public class AuthorEntity {

    @Id
    private UUID id;

    @Column(name = "name", nullable = false)
    private String name;

    protected AuthorEntity() {}

    public AuthorEntity(UUID id, String name) {
        this.id = id;
        this.name = name;
    }

    public UUID getId() { return id; }
    public String getName() { return name; }
}