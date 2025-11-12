
import re
from playwright.sync_api import Playwright, sync_playwright, expect

def run(playwright: Playwright) -> None:
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()

    # Listen for console events and print them
    page.on("console", lambda msg: print(f"CONSOLE LOG: {msg.text}"))

    try:
        page.goto("http://localhost:3001/")
        page.wait_for_load_state('networkidle')

        # Check if the login form is visible
        username_input = page.locator('input[name="username"]')
        if not username_input.is_visible():
            print("Login form not visible. Capturing screenshot.")
            page.screenshot(path="verification_error_login_not_visible.png")
        else:
            print("Login form is visible. Proceeding with login.")
            username_input.click()
            username_input.fill("admin")
            page.locator('input[name="password"]').click()
            page.locator('input[name="password"]').fill("123")
            page.get_by_role("button", name="Login").click()

            # Wait for navigation to the dashboard
            expect(page).to_have_url(re.compile(r".*#dashboard"))
            print("Successfully logged in.")

            # Navigate to the new "Criar O.S." page
            page.get_by_role("button", name="Ordem de Serviço").click()
            page.get_by_role("link", name="Criar O.S.").click()
            expect(page).to_have_url(re.compile(r".*#ordemServico"))
            print("Navigated to Criar O.S. page.")

            # Take a screenshot of the new page
            page.screenshot(path="/home/jules/verification/work_order_page.png")
            print("Screenshot of the work order page taken.")

    except Exception as e:
        print(f"An error occurred: {e}")
        page.screenshot(path="/home/jules/verification/verification_error.png")

    finally:
        context.close()
        browser.close()

with sync_playwright() as playwright:
    run(playwright)
