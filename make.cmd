@echo off

REM default target is test
if "%1" == "" (
    goto :test
)

2>NUL call :%1
if errorlevel 1 (
    echo Unknown target: %1
)

goto :end

:cover
    py.test --cov=pilosa tests integration_tests
    goto :end

:doc
    echo Generating documentation is not supported on this platform.
    goto :end

:generate
    echo Generating protobuf code is not supported on this platform.
    goto :end

:test
    py.test tests
    goto :end

:test-all
    py.test tests integration_tests
    goto :end

:end
