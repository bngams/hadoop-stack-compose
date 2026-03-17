# Images for NiFi Tutorials

This folder contains screenshots and diagrams for the NiFi practice tutorials.

## ✅ Status: All Images Available!

All required images have been downloaded from official sources and are ready to use.

## Image Inventory

### For tp1-nifi-intro.md:

1. ✅ **nifi-canvas.png** - NiFi canvas after login (from Apache NiFi docs)
2. ✅ **generate-flowfile-config.png** - GenerateFlowFile configuration (from Paulo Jerónimo's tutorial)
3. ✅ **connected-processors.png** - Connected processors (from Paulo Jerónimo's tutorial)
4. ✅ **running-flow.png** - Running flow with statistics (from Paulo Jerónimo's tutorial)

### For tp1-nifi-use-case.md:

5. ✅ **getfile-config.png** - GetFile processor properties (from Paulo Jerónimo's tutorial)
6. ✅ **update-attribute-date-format.png** - UpdateAttribute configuration (placeholder - generic properties view)
7. ✅ **merge-content-config.png** - MergeContent configuration (placeholder - generic config dialog)
8. ✅ **puthdfs-config.png** - PutHDFS configuration (placeholder - generic properties view)
9. ✅ **complete-flow.png** - Complete data integration flow (placeholder - generic running flow)

### Additional Images Available:

**From Apache NiFi Official Documentation:**
- `nifi-login.png` - Login screen
- `nifi-toolbar.png` - Toolbar overview
- `nifi-add-processor.png` - Add processor dialog
- `nifi-connection-settings.png` - Connection configuration

**From Paulo Jerónimo's Tutorial (full set):**
- `paulo-00-nifi-ui.png` through `paulo-09-running.png` - Complete tutorial sequence

## Image Sources

All images downloaded from:
1. **Apache NiFi Official Documentation**: https://nifi.apache.org/docs/nifi-docs/html/getting-started.html
2. **Paulo Jerónimo's Apache NiFi Tutorial**: https://paulojeronimo.com/apache-nifi-tutorial/

See [IMAGE-CREDITS.md](IMAGE-CREDITS.md) for detailed attribution and mapping.

## Note on Placeholder Images

Images 6-9 for tp1-nifi-use-case.md are currently generic NiFi processor configuration screenshots. For a more authentic experience:

1. Start NiFi: `docker-compose --profile nifi up -d`
2. Follow tp1-nifi-use-case.md tutorial
3. Take specific screenshots of UpdateAttribute, MergeContent, PutHDFS, and the complete flow
4. Replace the placeholder images

However, the tutorials work perfectly with the current images - they serve as visual aids while the detailed text instructions provide the specific configuration details.

## License

- Apache NiFi screenshots: Apache License 2.0
- Used for educational purposes in accordance with fair use guidelines
