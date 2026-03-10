import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { CustomSelect } from "./CustomSelect";

describe("CustomSelect", () => {
  it("renders selected value", () => {
    render(
      <CustomSelect
        value="b"
        options={[
          { value: "a", label: "Option A" },
          { value: "b", label: "Option B" },
        ]}
        onChange={vi.fn()}
      />
    );
    expect(screen.getByRole("button", { name: /Option B/i })).toBeInTheDocument();
  });

  it("opens dropdown on click and calls onChange when option selected", () => {
    const onChange = vi.fn();
    render(
      <CustomSelect
        value="a"
        options={[
          { value: "a", label: "Option A" },
          { value: "b", label: "Option B" },
        ]}
        onChange={onChange}
      />
    );
    fireEvent.click(screen.getByRole("button"));
    expect(screen.getByText("Option B")).toBeInTheDocument();
    fireEvent.click(screen.getByText("Option B"));
    expect(onChange).toHaveBeenCalledWith("b");
  });

  it("shows search input when searchable", () => {
    render(
      <CustomSelect
        value="a"
        options={[
          { value: "a", label: "Apple" },
          { value: "b", label: "Banana" },
        ]}
        onChange={vi.fn()}
        searchable
      />
    );
    fireEvent.click(screen.getByRole("button"));
    expect(screen.getByPlaceholderText("Search...")).toBeInTheDocument();
  });
});
