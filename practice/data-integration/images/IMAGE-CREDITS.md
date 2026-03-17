# Image Credits and Mapping

This folder contains screenshots for the NiFi data integration tutorials.

## Image Sources

### From Paulo Jerónimo's Apache NiFi Tutorial
Source: https://paulojeronimo.com/apache-nifi-tutorial/

Downloaded images (with paulo- prefix):
- `paulo-00-nifi-ui.png` - NiFi user interface overview
- `paulo-01-add-processor.png` - Adding a processor dialog
- `paulo-02-generate-flowfile.png` - GenerateFlowFile processor configuration
- `paulo-03-configure-processor.png` - Processor configuration dialog
- `paulo-04-properties.png` - Processor properties tab
- `paulo-05-scheduling.png` - Processor scheduling settings
- `paulo-06-connection.png` - Creating connections between processors
- `paulo-07-logattribute.png` - LogAttribute processor configuration
- `paulo-08-connected.png` - Connected processors on canvas
- `paulo-09-running.png` - Running flow with statistics

### From Apache NiFi Official Documentation
Source: https://nifi.apache.org/docs/nifi-docs/html/getting-started.html

Downloaded images (with nifi- prefix):
- `nifi-login.png` - NiFi login screen
- `nifi-canvas.png` - Main NiFi canvas after authentication
- `nifi-toolbar.png` - Toolbar components overview
- `nifi-add-processor.png` - Add processor interface
- `nifi-connection-settings.png` - Connection configuration dialog

## Tutorial Image Mappings

### For tp1-nifi-intro.md

| Referenced in Tutorial | Actual File | Description |
|------------------------|-------------|-------------|
| `nifi-canvas.png` | `nifi-canvas.png` ✅ | NiFi UI overview |
| `generate-flowfile-config.png` | `generate-flowfile-config.png` ✅ (copy of paulo-02) | GenerateFlowFile configuration |
| `connected-processors.png` | `connected-processors.png` ✅ (copy of paulo-08) | Processors with connection |
| `running-flow.png` | `running-flow.png` ✅ (copy of paulo-09) | Flow executing with stats |

### For tp1-nifi-use-case.md

| Referenced in Tutorial | Actual File | Description |
|------------------------|-------------|-------------|
| `getfile-config.png` | `getfile-config.png` ✅ (copy of paulo-04) | GetFile processor properties |
| `update-attribute-date-format.png` | ⚠️ *To be created* | UpdateAttribute for dates |
| `merge-content-config.png` | ⚠️ *To be created* | MergeContent configuration |
| `puthdfs-config.png` | ⚠️ *To be created* | PutHDFS configuration |
| `complete-flow.png` | ⚠️ *To be created* | Full employee data flow |

## Missing Images (To Be Generated)

The following images should be created by following the tp1-nifi-use-case.md tutorial:

1. **update-attribute-date-format.png**
   - Screenshot: UpdateAttribute processor with custom properties for date validation
   - Alternative: Use `paulo-04-properties.png` as reference

2. **merge-content-config.png**
   - Screenshot: MergeContent processor with Bin-Packing and delimiter settings
   - Can be generated during tutorial execution

3. **puthdfs-config.png**
   - Screenshot: PutHDFS processor with Hadoop config and directory settings
   - Can be generated during tutorial execution

4. **complete-flow.png**
   - Screenshot: The complete multi-file integration flow on canvas
   - Can be generated at the end of the tutorial

## Generating Missing Images

### Option 1: Instructor/Student Generated
Follow the tp1-nifi-use-case.md tutorial and take screenshots at the indicated steps.

### Option 2: Use Generic Images
The tutorials can work with the available generic images:
- Use `paulo-04-properties.png` for any processor properties configuration
- Use `paulo-08-connected.png` for any flow layout
- Use `paulo-09-running.png` for any running flow

### Option 3: Placeholder Text
The detailed written instructions are sufficient - images are supplementary visual aids.

## File Organization

```
images/
├── IMAGE-CREDITS.md (this file)
├── README.md (image generation guide)
│
├── Official NiFi Docs Images:
│   ├── nifi-canvas.png
│   ├── nifi-login.png
│   ├── nifi-toolbar.png
│   ├── nifi-add-processor.png
│   └── nifi-connection-settings.png
│
├── Paulo Jerónimo Tutorial Images:
│   ├── paulo-00-nifi-ui.png
│   ├── paulo-01-add-processor.png
│   ├── paulo-02-generate-flowfile.png
│   ├── paulo-03-configure-processor.png
│   ├── paulo-04-properties.png
│   ├── paulo-05-scheduling.png
│   ├── paulo-06-connection.png
│   ├── paulo-07-logattribute.png
│   ├── paulo-08-connected.png
│   └── paulo-09-running.png
│
└── Tutorial-Specific Names (copies/symlinks):
    ├── generate-flowfile-config.png
    ├── connected-processors.png
    ├── running-flow.png
    └── getfile-config.png
```

## License and Usage

- Apache NiFi screenshots are from official Apache NiFi documentation (Apache License 2.0)
- Paulo Jerónimo tutorial images are used for educational purposes
- All images used in accordance with fair use for educational material

For production use or public distribution, verify licensing terms with original sources.

## Recommended Action

For a complete tutorial experience:
1. Start NiFi: `docker-compose --profile nifi up -d`
2. Follow tp1-nifi-use-case.md step-by-step
3. Take screenshots of the 4 missing images
4. Save them with the exact filenames listed above
5. All tutorials will then have complete visual documentation

Alternatively, the tutorials work perfectly well with just the text instructions - images are supplementary aids for visual learners.
