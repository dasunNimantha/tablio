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

  // ---------------------------------------------------------------
  // portal mode (issue #62 follow-up): the dropdown must render
  // outside the wrapper subtree so it can escape ancestor
  // `overflow: hidden`. Default mode keeps the dropdown inline.
  // ---------------------------------------------------------------

  it("renders the dropdown inline (as a wrapper descendant) when portal is off", () => {
    const { container } = render(
      <CustomSelect
        value="a"
        options={[
          { value: "a", label: "A" },
          { value: "b", label: "B" },
        ]}
        onChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByRole("button"));
    // The wrapper is the first `.cs-wrapper` in the render output —
    // the dropdown must be inside it.
    const wrapper = container.querySelector(".cs-wrapper");
    expect(wrapper).not.toBeNull();
    expect(wrapper!.querySelector(".cs-dropdown")).not.toBeNull();
    expect(
      wrapper!.querySelector(".cs-dropdown--portal"),
    ).toBeNull();
  });

  it("renders the dropdown via a portal (escaping the wrapper) when portal is on", () => {
    const { container } = render(
      <CustomSelect
        value="a"
        options={[
          { value: "a", label: "A" },
          { value: "b", label: "B" },
        ]}
        onChange={vi.fn()}
        portal
      />,
    );
    fireEvent.click(screen.getByRole("button"));
    const wrapper = container.querySelector(".cs-wrapper");
    // The wrapper subtree must NOT contain the dropdown — it
    // lives elsewhere in the document (createPortal target).
    expect(wrapper!.querySelector(".cs-dropdown")).toBeNull();
    // The portal-marker class is what global CSS uses to apply
    // the `z-index: 2000` + width-recovery rules.
    const portalDropdown = document.querySelector(".cs-dropdown--portal");
    expect(portalDropdown).not.toBeNull();
  });

  it("portal-mode dropdown still emits onChange when an option is clicked", () => {
    // Click-routing breaks if the click-outside listener treats the
    // portalised dropdown as "outside" — this test guards against
    // that regression by asserting an option click survives the
    // mousedown phase and onChange fires.
    const onChange = vi.fn();
    render(
      <CustomSelect
        value="a"
        options={[
          { value: "a", label: "A" },
          { value: "b", label: "B" },
        ]}
        onChange={onChange}
        portal
      />,
    );
    fireEvent.click(screen.getByRole("button"));
    fireEvent.click(screen.getByText("B"));
    expect(onChange).toHaveBeenCalledWith("b");
  });
});
