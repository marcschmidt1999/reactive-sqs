#!/usr/bin/env bash

set -Eeuo pipefail

LIB_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
    cat <<'USAGE'
Usage: ./run.sh <command> [argument]

Commands:
  format                 Apply project formatting.
  check                  Run tests and static checks.
  build                  Build every module.
  versions               List available dependency, plugin, and Gradle updates.
  publish-local          Publish artifacts to the local Maven repository.
  tag <version>          Create a local annotated Git tag named v<version>.
  help                   Show this help.
USAGE
}

die() {
    printf 'error: %s\n' "$*" >&2
    exit 1
}

require_no_arguments() {
    [[ $# -eq 0 ]] || die "this command does not take arguments"
}

run_gradle() {
    (
        cd "$LIB_ROOT"
        ./gradlew "$@"
    )
}

create_tag() {
    [[ $# -eq 1 ]] || die "usage: ./run.sh tag <version>"

    local version="$1"
    [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]] \
        || die "version must look like 1.2.3"

    command -v git >/dev/null 2>&1 || die "Git is required to create a tag"
    git -C "$LIB_ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1 \
        || die "run.sh tag requires a Git repository"

    local changes tag
    changes="$(git -C "$LIB_ROOT" status --porcelain)"
    [[ -z "$changes" ]] || die "commit or remove local changes before creating a tag"

    tag="v$version"
    git -C "$LIB_ROOT" show-ref --tags --verify --quiet "refs/tags/$tag" \
        && die "tag already exists: $tag"
    git -C "$LIB_ROOT" tag -a "$tag" -m "Release $tag"
    printf 'Created local tag %s\n' "$tag"
}

run() {
    local command="${1:-help}"
    if (($#)); then
        shift
    fi

    case "$command" in
        help|-h|--help)
            require_no_arguments "$@"
            usage
            ;;
        format)
            require_no_arguments "$@"
            run_gradle spotlessApply
            ;;
        check)
            require_no_arguments "$@"
            run_gradle check
            ;;
        build)
            require_no_arguments "$@"
            run_gradle build
            ;;
        versions)
            require_no_arguments "$@"
            run_gradle --no-parallel --no-configuration-cache --dependency-verification off dependencyUpdates
            ;;
        publish-local)
            require_no_arguments "$@"
            run_gradle publishToMavenLocal
            ;;
        tag)
            create_tag "$@"
            ;;
        *)
            die "unknown command: $command"
            ;;
    esac
}
