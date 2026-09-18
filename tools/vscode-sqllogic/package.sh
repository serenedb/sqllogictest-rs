#!/usr/bin/env bash
# Builds sqllogic-<version>.vsix next to this script.
#
# Does not use @vscode/vsce: a .vsix is just a zip, and current vsce needs a
# newer node than the one on most dev boxes here.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
version="$(sed -n 's/.*"version": "\(.*\)".*/\1/p' "$here/package.json" | head -1)"
out="$here/sqllogic-$version.vsix"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

mkdir -p "$work/extension"
cp -r "$here/package.json" "$here/language-configuration.json" "$here/README.md" \
	"$here/syntaxes" "$work/extension/"

cat >"$work/extension.vsixmanifest" <<EOF
<?xml version="1.0" encoding="utf-8"?>
<PackageManifest Version="2.0.0" xmlns="http://schemas.microsoft.com/developer/vsx-schema/2011" xmlns:d="http://schemas.microsoft.com/developer/vsx-schema-design/2011">
  <Metadata>
    <Identity Language="en-US" Id="sqllogic" Version="$version" Publisher="serenedb" />
    <DisplayName>SQLLogicTest</DisplayName>
    <Description xml:space="preserve">Syntax highlighting for sqllogictest-rs test files</Description>
    <Tags>sql,sqllogictest,syntax</Tags>
    <Categories>Programming Languages</Categories>
    <GalleryFlags>Public</GalleryFlags>
    <Properties>
      <Property Id="Microsoft.VisualStudio.Code.Engine" Value="^1.75.0" />
      <Property Id="Microsoft.VisualStudio.Code.ExtensionDependencies" Value="" />
      <Property Id="Microsoft.VisualStudio.Code.ExtensionPack" Value="" />
      <Property Id="Microsoft.VisualStudio.Code.ExtensionKind" Value="ui,workspace" />
      <Property Id="Microsoft.VisualStudio.Code.LocalizedLanguages" Value="" />
    </Properties>
  </Metadata>
  <Installation>
    <InstallationTarget Id="Microsoft.VisualStudio.Code" />
  </Installation>
  <Dependencies/>
  <Assets>
    <Asset Type="Microsoft.VisualStudio.Code.Manifest" Path="extension/package.json" Addressable="true" />
    <Asset Type="Microsoft.VisualStudio.Services.Content.Details" Path="extension/README.md" Addressable="true" />
  </Assets>
</PackageManifest>
EOF

cat >"$work/[Content_Types].xml" <<'EOF'
<?xml version="1.0" encoding="utf-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="json" ContentType="application/json" />
  <Default Extension="vsixmanifest" ContentType="text/xml" />
  <Default Extension="xml" ContentType="text/xml" />
  <Default Extension="md" ContentType="text/markdown" />
</Types>
EOF

rm -f "$out"
(cd "$work" && zip -q -r "$out" .)
echo "$out"
