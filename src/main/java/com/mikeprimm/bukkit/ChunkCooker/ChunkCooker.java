package com.mikeprimm.bukkit.ChunkCooker;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Logger;

import org.bukkit.Chunk;
import org.bukkit.World;
import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.world.ChunkUnloadEvent;
import org.bukkit.event.world.WorldUnloadEvent;
import org.bukkit.plugin.java.JavaPlugin;

public class ChunkCooker extends JavaPlugin {
    public Logger log;
    
    private int cooker_period = 30; // 30 seconds
    private int chunks_per_period = 100;
    private boolean storm_on_empty = true;
    
    private int worldIndex = 0;
    private World currentWorld = null;
    private HashSet<TileFlags.TileCoord> loadedChunks = new HashSet<TileFlags.TileCoord>();
    private TileFlags chunkmap = new TileFlags();
    private TileFlags.Iterator iter;
    private boolean stormset;
    
    private void tickCooker() {
        // If any chunks loaded from last tick, unload them
        if (loadedChunks.isEmpty() == false) {
            TileFlags.TileCoord[] tounload = loadedChunks.toArray(new TileFlags.TileCoord[0]);
            loadedChunks.clear(); // Remove all - keeps unload from being blocked
            for (TileFlags.TileCoord c : tounload) {
                if (currentWorld.isChunkInUse(c.x, c.y) == false) {
                    currentWorld.unloadChunkRequest(c.x, c.y);
                }
            }
        }
        // If iterator is exhausted, done with current world
        if ((iter != null) && (iter.hasNext() == false)) {
            iter = null;
            if (currentWorld != null) {
                log.info("World '" + currentWorld.getName() + "' - chunk cooking completed");
            }
            currentWorld = null;
        }
        /* If no active world, select next one */
        if (currentWorld == null) {
            List<World> w = this.getServer().getWorlds();
            int wcnt = w.size();
            for (int i = 0; i < wcnt; i++) {
                int idx = worldIndex % wcnt;
                worldIndex++;
                currentWorld = w.get(idx);
                if (currentWorld.getEnvironment() == World.Environment.NORMAL) {
                    stormset = false;
                    break;
                }
                currentWorld = null;
            }
            // If no world selected, no normal worlds right now
            if (currentWorld == null) {
                return;
            }
            // Now, get current chunk map for world
            int ccnt = getChunkMap(currentWorld, chunkmap);
            log.info("Starting cook pass for world '" + currentWorld.getName() + "' - " + ccnt + " existing chunks");
            iter = chunkmap.getIterator();  // Get iterator
        }
        // Now, load next N chunks (and their neighbors)
        TileFlags.TileCoord tc = new TileFlags.TileCoord();
        while(iter.hasNext() && (loadedChunks.size() < chunks_per_period)) {
            iter.next(tc);
            int x0 = tc.x;
            int z0 = tc.y;
            // Try to load chunk, and its 8 neighbors
            for (tc.x = x0 - 1; tc.x <= x0 + 1; tc.x++) {
                for (tc.y = z0 - 1; tc.y <= z0 + 1; tc.y++) {
                    if(loadedChunks.contains(tc) == false) { // Not in loaded set yet 
                        if(currentWorld.isChunkLoaded(tc.x, tc.y)) { // Already loaded
                            loadedChunks.add(new TileFlags.TileCoord(tc.x, tc.y));
                        }
                        else if(currentWorld.loadChunk(tc.x, tc.y, false)) { // Was able to be loaded without generating
                            loadedChunks.add(new TileFlags.TileCoord(tc.x, tc.y));
                        }
                    }
                }
            }
        }
        if (storm_on_empty) {
            if (currentWorld.getPlayers().isEmpty()) { // If world is empty
                if (currentWorld.hasStorm() == false) {
                    currentWorld.setStorm(true);
                    stormset = true;
                    log.info("Setting storm on empty world '" + currentWorld.getName() + "'");
                }
            }
            else {
                if (stormset) {
                    currentWorld.setStorm(false);
                    stormset = false;
                }
            }
        }
        log.info(loadedChunks.size() + " chunks loaded for cooking");
    }
    
    /* On disable, stop doing our function */
    public void onDisable() {
        
    }

    public void onEnable() {
        log = this.getLogger();
        
        log.info("ChunkCooker loaded");
        
        FileConfiguration cfg = getConfig();
        cfg.options().copyDefaults(true);   /* Load defaults, if needed */
        this.saveConfig();  /* Save updates, if needed */
        
        chunks_per_period = cfg.getInt("chunks-per-period", 100);
        if (chunks_per_period < 1) chunks_per_period = 1;
        cooker_period = cfg.getInt("seconds-per-period", 30);
        if(cooker_period < 5) cooker_period = 5;
        storm_on_empty = cfg.getBoolean("storm-on-empty-world", true);

        this.getServer().getScheduler().scheduleSyncRepeatingTask(this, new Runnable() {
            public void run() {
                tickCooker();
            }
        }, cooker_period * 20, cooker_period * 20);
        
        Listener pl = new Listener() {
            @EventHandler(priority=EventPriority.NORMAL)
            public void onChunkUnload(ChunkUnloadEvent evt) {
                if(evt.isCancelled()) return;
                Chunk c = evt.getChunk();
                if(c.getWorld() == currentWorld) {
                    TileFlags.TileCoord tc = new TileFlags.TileCoord(c.getX(), c.getZ());
                    if (loadedChunks.contains(tc)) { // If loaded, cancel unload
                        evt.setCancelled(true);
                    }
                }
            }
            @EventHandler(priority=EventPriority.MONITOR)
            public void onWorldUnload(WorldUnloadEvent evt) {
                if (evt.isCancelled()) return;
                if (evt.getWorld() == currentWorld) {
                    log.info("World '" + currentWorld.getName() + "' unloaded - chunk cooking cancelled");
                    currentWorld = null;
                    iter = null;
                    loadedChunks.clear();
                    chunkmap.clear();
                }
            }
        };
        getServer().getPluginManager().registerEvents(pl, this);
    }

    @Override
    public boolean onCommand(CommandSender sender, Command cmd, String commandLabel, String[] args) {
        return false;
    }

    public int getChunkMap(World world, TileFlags map) {
        map.clear();
        if (world == null) return -1;
        int cnt = 0;
        // Mark loaded chunks
        for(Chunk c : world.getLoadedChunks()) {
            map.setFlag(c.getX(), c.getZ(), true);
            cnt++;
        }
        File f = world.getWorldFolder();
        File regiondir = new File(f, "region");
        File[] lst = regiondir.listFiles();
        if(lst != null) {
            byte[] hdr = new byte[4096];
            for(File rf : lst) {
                if(!rf.getName().endsWith(".mca")) {
                    continue;
                }
                String[] parts = rf.getName().split("\\.");
                if((!parts[0].equals("r")) && (parts.length != 4)) continue;
                
                RandomAccessFile rfile = null;
                int x = 0, z = 0;
                try {
                    x = Integer.parseInt(parts[1]);
                    z = Integer.parseInt(parts[2]);
                    rfile = new RandomAccessFile(rf, "r");
                    rfile.read(hdr, 0, hdr.length);
                } catch (IOException iox) {
                    Arrays.fill(hdr,  (byte)0);
                } catch (NumberFormatException nfx) {
                    Arrays.fill(hdr,  (byte)0);
                } finally {
                    if(rfile != null) {
                        try { rfile.close(); } catch (IOException iox) {}
                    }
                }
                for (int i = 0; i < 1024; i++) {
                    int v = hdr[4*i] | hdr[4*i + 1] | hdr[4*i + 2] | hdr[4*i + 3];
                    if (v == 0) continue;
                    int xx = (x << 5) | (i & 0x1F);
                    int zz = (z << 5) | ((i >> 5) & 0x1F);
                    if (!map.setFlag(xx, zz, true)) {
                        cnt++;
                    }
                }
            }
        }
        return cnt;
    }

}
