package com.bullit.web.adapter.driving.http;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;
import java.util.UUID;

public class Request {
    public record AddBookRequest(
            @Size(max = 200, message = "Book title can be 200 characters at most")
            @NotBlank(message = "Book title is required")
            String title
    ) {}

    public record CreateAuthorRequest(
            @Size(max = 100, message = "Author first name can be 100 characters at most")
            @NotBlank(message = "Author first name is required")
            String firstName,
            @Size(max = 100, message = "Author last name can be 100 characters at most")
            @NotBlank(message = "Author last name is required")
            String lastName
    ) {}

    public record CreateSaleRequest(
            @NotNull(message = "Book ID is required")
            UUID bookId,
            @Positive(message = "You can only sell a positive number of books")
            int units,
            @NotNull(message = "Sale amount is required")
            @Positive(message = "Sale amount must be higher than zero")
            BigDecimal amountEur
    ) {}
}
