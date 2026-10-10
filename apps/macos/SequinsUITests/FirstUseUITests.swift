import XCTest

final class FirstUseUITests: XCTestCase {
    private var testHome: URL!

    private func makeApp() -> XCUIApplication {
        let app = XCUIApplication()
        // Isolate SwiftData, telemetry storage, and preferences from the user's app.
        app.launchEnvironment["CFFIXED_USER_HOME"] = testHome.path
        return app
    }

    override func setUpWithError() throws {
        continueAfterFailure = false
        testHome = FileManager.default.temporaryDirectory.appendingPathComponent("SequinsUITests-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: testHome, withIntermediateDirectories: true)
    }

    override func tearDownWithError() throws {
        XCUIApplication().terminate()
        try FileManager.default.removeItem(at: testHome)
    }

    @MainActor
    func testForegroundLaunchShowsMainWindow() throws {
        let app = makeApp()
        app.launchArguments = ["-showMainWindowOnLaunch", "YES"]
        app.launch()
        XCTAssertTrue(app.windows.firstMatch.waitForExistence(timeout: 10))
    }

    @MainActor
    func testBackgroundLaunchDoesNotShowMainWindow() throws {
        let app = makeApp()
        app.launchArguments = ["-showMainWindowOnLaunch", "NO"]
        app.launch()
        XCTAssertFalse(app.windows.firstMatch.waitForExistence(timeout: 3))
        XCTAssertEqual(app.state, .runningBackground)
    }

    @MainActor
    func testQAOverrideOpensWindowInBackgroundMode() throws {
        let app = makeApp()
        app.launchArguments = ["-showMainWindowOnLaunch", "NO"]
        app.launchEnvironment["SEQUINS_AUTO_OPEN_WINDOW"] = "Health"
        app.launch()
        XCTAssertTrue(app.windows.firstMatch.waitForExistence(timeout: 10))
    }

    @MainActor
    func testWelcomeCanBeDismissed() throws {
        let app = makeApp()
        app.launchArguments = ["-showMainWindowOnLaunch", "YES", "-hasAcknowledgedWelcome", "NO"]
        app.launch()
        let welcome = app.staticTexts["Welcome to Sequins"]
        XCTAssertTrue(welcome.waitForExistence(timeout: 10))
        app.buttons["Got it"].click()
        XCTAssertFalse(welcome.exists)
        // The main window remains usable after acknowledging the welcome.
        XCTAssertTrue(app.windows.firstMatch.exists)
    }

    @MainActor
    func testWelcomeLinksToStartupPreference() throws {
        let app = makeApp()
        app.launchArguments = ["-showMainWindowOnLaunch", "YES", "-hasAcknowledgedWelcome", "NO"]
        app.launch()
        let settings = app.buttons["Open Settings"]
        XCTAssertTrue(settings.waitForExistence(timeout: 10))
        settings.click()
        XCTAssertTrue(app.checkBoxes["Show main window when starting"].waitForExistence(timeout: 5))
    }

    @MainActor
    func testEmptyLocalEnvironmentExplainsHowToSendTelemetry() throws {
        let app = makeApp()
        app.launchArguments = ["-showMainWindowOnLaunch", "YES", "-hasAcknowledgedWelcome", "YES"]
        app.launch()
        XCTAssertTrue(app.staticTexts["Send your first telemetry"].waitForExistence(timeout: 10))
        XCTAssertTrue(app.buttons["Copy exporter configuration"].exists)
        let attachment = XCTAttachment(screenshot: app.screenshot())
        attachment.lifetime = .keepAlways
        add(attachment)
    }

    @MainActor
    func testClosingWindowKeepsCollectorRunningAndMenuBarCanReopenIt() async throws {
        let app = makeApp()
        app.launchArguments = ["-showMainWindowOnLaunch", "YES", "-hasAcknowledgedWelcome", "YES"]
        app.launch()
        XCTAssertTrue(app.staticTexts["Send your first telemetry"].waitForExistence(timeout: 10))
        app.windows.firstMatch.buttons[XCUIIdentifierCloseWindow].click()
        XCTAssertFalse(app.windows.firstMatch.exists)

        // A successful OTLP response proves that closing the UI leaves ingestion active.
        var request = URLRequest(url: URL(string: "http://localhost:4318/v1/traces")!)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = Data("{\"resourceSpans\":[]}".utf8)
        let (_, response) = try await URLSession.shared.data(for: request)
        XCTAssertEqual((response as? HTTPURLResponse)?.statusCode, 200)

        let statusItem = app.descendants(matching: .statusItem).firstMatch
        XCTAssertTrue(statusItem.exists)
        if !statusItem.isHittable {
            // Reveal an automatically hidden menu bar without changing system settings.
            statusItem.coordinate(withNormalizedOffset: CGVector(dx: 0.5, dy: 0))
                .withOffset(CGVector(dx: 0, dy: 1 - statusItem.frame.minY)).hover()
            let visible = XCTNSPredicateExpectation(predicate: NSPredicate(format: "hittable == true"), object: statusItem)
            XCTAssertEqual(XCTWaiter.wait(for: [visible], timeout: 5), .completed)
        }
        statusItem.click()
        let open = app.buttons["Open Sequins"]
        XCTAssertTrue(open.waitForExistence(timeout: 5))
        open.click()
        XCTAssertTrue(app.windows.firstMatch.waitForExistence(timeout: 5))
    }
}
