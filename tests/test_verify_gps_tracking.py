
import pytest
from playwright.async_api import async_playwright, expect

@pytest.mark.skip(reason="GPS tracking requires permissions not available in test env")
@pytest.mark.asyncio
async def test_automatic_gps_tracking():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()

        await page.goto("http://localhost:8000")

        # Mock the currentUser and trigger the app screen
        await page.evaluate("""() => {
            window.App.state.currentUser = {
                uid: 'mock_uid',
                email: 'test@example.com',
                username: 'Test User',
                role: 'admin',
                companyId: 'mock_company_id',
                permissions: { dashboard: true, monitoramentoAereo: true }
            };
            window.App.ui.showAppScreen();
        }""")

        # Wait for the app to initialize and check the tracking state
        await page.wait_for_timeout(2000) # Give it a moment to run the startup logic

        is_tracking = await page.evaluate("() => window.App.state.isTracking")
        assert is_tracking is True, "GPS tracking did not start automatically"

        await page.screenshot(path="jules-scratch/verification/verification.png")

        await browser.close()
