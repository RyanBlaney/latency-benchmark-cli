#!/bin/bash

# Comprehensive Go Linting Script
set -e

PROJECT_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
MODULE_NAME=$(go list -m 2>/dev/null || echo "")

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_step() {
    echo -e "${BLUE}🔍 $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Function to run a command and capture its exit code
run_check() {
    local name="$1"
    local cmd="$2"
    local fix_mode="$3"
    
    print_step "Running $name..."
    
    if [ "$fix_mode" = "fix" ]; then
        if eval "$cmd"; then
            print_success "$name passed"
            return 0
        else
            print_error "$name failed"
            return 1
        fi
    else
        # Check mode - capture output but don't fail immediately
        local output
        local exit_code
        
        output=$(eval "$cmd" 2>&1) || exit_code=$?
        
        if [ ${exit_code:-0} -eq 0 ]; then
            print_success "$name passed"
            return 0
        else
            print_error "$name failed"
            echo "$output"
            return 1
        fi
    fi
}

# Parse command line arguments
MODE="check"
VERBOSE=false
FILES=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --fix|-f)
            MODE="fix"
            shift
            ;;
        --check|-c)
            MODE="check"
            shift
            ;;
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        --files)
            FILES="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "OPTIONS:"
            echo "  --fix, -f       Fix issues automatically where possible"
            echo "  --check, -c     Check only (default)"
            echo "  --verbose, -v   Verbose output"
            echo "  --files PATH    Check specific files/directories"
            echo "  --help, -h      Show this help"
            exit 0
            ;;
        *)
            echo "Unknown option $1"
            exit 1
            ;;
    esac
done

# Set target files/directories
TARGET=${FILES:-"./..."}
if [ "$FILES" != "" ]; then
    TARGET="$FILES"
fi

echo "🚀 Running Go linting suite in $MODE mode..."
echo "📁 Target: $TARGET"
echo "📦 Module: $MODULE_NAME"
echo ""

FAILED=0

# 1. Format imports with gci
if [ "$MODE" = "fix" ]; then
    if [ "$MODULE_NAME" != "" ]; then
        run_check "GCI (import formatting)" \
            "gci write --section standard --section default --section 'prefix($MODULE_NAME)' ." \
            "fix" || ((FAILED++))
    else
        run_check "GCI (import formatting)" \
            "gci write --section standard --section default ." \
            "fix" || ((FAILED++))
    fi
else
    if [ "$MODULE_NAME" != "" ]; then
        run_check "GCI (import formatting)" \
            "gci diff --section standard --section default --section 'prefix($MODULE_NAME)' ." \
            "check" || ((FAILED++))
    else
        run_check "GCI (import formatting)" \
            "gci diff --section standard --section default ." \
            "check" || ((FAILED++))
    fi
fi

# 2. Format code with gofumpt
if [ "$MODE" = "fix" ]; then
    run_check "gofumpt (code formatting)" \
        "gofumpt -w ." \
        "fix" || ((FAILED++))
else
    run_check "gofumpt (code formatting)" \
        "gofumpt -d ." \
        "check" || ((FAILED++))
fi

# 3. Run golangci-lint
if [ "$MODE" = "fix" ]; then
    run_check "golangci-lint" \
        "golangci-lint run --fix $TARGET" \
        "fix" || ((FAILED++))
else
    run_check "golangci-lint" \
        "golangci-lint run $TARGET" \
        "check" || ((FAILED++))
fi

# 4. Run go mod tidy
run_check "go mod tidy" \
    "go mod tidy" \
    "fix" || ((FAILED++))

# 5. Run go vet
run_check "go vet" \
    "go vet $TARGET" \
    "check" || ((FAILED++))

# 6. Run staticcheck (if available)
if command -v staticcheck &> /dev/null; then
    run_check "staticcheck" \
        "staticcheck $TARGET" \
        "check" || ((FAILED++))
fi

# 7. Run gosec (if available)
if command -v gosec &> /dev/null; then
    run_check "gosec (security)" \
        "gosec -quiet ./..." \
        "check" || ((FAILED++))
fi

# 8. Check for misspellings (if available)
if command -v misspell &> /dev/null; then
    run_check "misspell" \
        "misspell -error ." \
        "check" || ((FAILED++))
fi

# 9. Vulnerability check (if available)
if command -v govulncheck &> /dev/null; then
    run_check "govulncheck (vulnerabilities)" \
        "govulncheck ./..." \
        "check" || ((FAILED++))
fi

echo ""
if [ $FAILED -eq 0 ]; then
    print_success "All checks passed! 🎉"
    exit 0
else
    print_error "$FAILED check(s) failed"
    echo ""
    echo "💡 To fix automatically where possible, run:"
    echo "   $0 --fix"
    exit 1
fi
