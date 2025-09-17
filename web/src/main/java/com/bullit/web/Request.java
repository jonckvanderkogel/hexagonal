package com.bullit.web;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public class Request {
    public record AddBookRequest(
            @NotBlank @Size(max = 200) String title
    ) {}

    public record CreateAuthorRequest(
            @NotBlank @Size(max = 100) String firstName,
            @NotBlank @Size(max = 100) String lastName
    ) {}
}
