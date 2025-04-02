import subprocess
import time
from datetime import datetime


#test parameter festlegen
POOL_NAME = "mypool"
MOUNTPOINT = "/mnt/draidBenchmark"
FILL_LEVELS = [0.01]
SPARES = 1
NUMJOBS_LIST = [1, 4, 8, 16, 32, 64, 128]

def run_cmd(cmd, check=True):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f"[FEHLER] Befehl fehlgeschlagen: {cmd}")
        print(result.stderr)
    return result.stdout.strip()

#cashing ausstellen um geschwindigkeit nicht zu verzerren
def tune_cache_for_benchmark():
    print("[INFO] Setze aggressive Cache-Settings...")
    cmds = [
        "sysctl -w vm.dirty_ratio=2",
        "sysctl -w vm.dirty_background_ratio=1",
        "sysctl -w vm.dirty_expire_centisecs=100",
        "sysctl -w vm.dirty_writeback_centisecs=100"
    ]
    for cmd in cmds:
        subprocess.run(cmd, shell=True)

def restore_cache_settings():
    print("[INFO] Stelle Cache-Settings zurück...")
    cmds = [
        "sysctl -w vm.dirty_ratio=20",
        "sysctl -w vm.dirty_background_ratio=10",
        "sysctl -w vm.dirty_expire_centisecs=3000",
        "sysctl -w vm.dirty_writeback_centisecs=500"
    ]
    for cmd in cmds:
        subprocess.run(cmd, shell=True)



#geht besser aber funktioniert
def get_valid_disk_paths():
    cmd = r'''
for dev in /dev/sd*
do
    [[ "$dev" =~ [0-9] ]] && continue
    id=$(smartctl -i "$dev" 2>/dev/null | grep 'Logical Unit id' | awk '{print $4}')
    if [[ ${#id} -eq 18 ]]; then
        for prefix in /dev/disk/by-id/wwn-*
        do
            if [[ -e "$prefix" ]] && [[ "$(readlink -f "$prefix")" == "$(readlink -f "$dev")" ]]; then
                echo "$prefix"
                break
            fi
        done
    fi
done
'''
    result = subprocess.run(["bash", "-c", cmd], capture_output=True, text=True)
    if result.returncode != 0:
        print("Fehler beim Abrufen der Disk-Pfade:")
        print(result.stderr)
        return []
    lines = result.stdout.strip().splitlines()
    print(f"[INFO] {len(lines)} gültige Disks gefunden.")
    return lines

def generate_rg_configs(dev_paths):
    total_disks = len(dev_paths)
    configs = []

    children = total_disks
    spares = SPARES
    parity = 2
    min_data = 1
    max_data = children - spares - parity

    for data in range(min_data, max_data + 1):
        if (children - parity - spares) % data != 0:
            continue

        vdev_config = f"draid2:{data}d:{spares}s:{children}c"
        vdev_devs = " ".join(dev_paths)
        zpool_cmd = ( #potentielles optimieren hier
            f"zpool create -f -m {MOUNTPOINT} -o ashift=12 {POOL_NAME} \\\n"
            f"  {vdev_config} {vdev_devs}"
        )

        configs.append({
            "vdevs": 1,
            "children": children,
            "spares": spares,
            "parity": parity,
            "data": data,
            "zfs_syntax": vdev_config,
            "zpool_create_cmd": zpool_cmd,
            "used_disks": dev_paths
        })

    configs.sort(key=lambda x: x["data"])
    return configs

def create_pool(pool_cmd, used_disks):
    print("[INFO] Wipe alte Metadaten von Disks...")
    for disk in used_disks:
        run_cmd(f"wipefs -a {disk}", check=False)

    print("[INFO] Erstelle Pool...")
    run_cmd(pool_cmd)
    print("[INFO] Deaktiviere Kompression...")
    run_cmd(f"zfs set compression=off {POOL_NAME}")

def fill_pool(level, numjobs):
    print(f"[INFO] Fülle Pool zu {int(level * 100)}% mit fio, numjobs={numjobs}...")

    output = run_cmd(f"zfs list -Hp -o available {POOL_NAME}")
    available_bytes = int(output.strip())
    fill_size_bytes = int(available_bytes * level)
    fill_size_gib = fill_size_bytes // (1024 ** 3)

    print(f"[INFO] Zielgröße gesamt: {fill_size_gib} GiB")

    if fill_size_gib == 0:
        print("[INFO] Kein Füllbedarf, überspringe fio.")
        return

    per_file_gib = max(1, fill_size_gib // numjobs)
    filenames = [f"{MOUNTPOINT}/fillfile_{i}" for i in range(numjobs)]
    fio_filename_str = ":".join(filenames)

    fio_cmd = ( #potentielles optimieren hier
        f"fio --name=filljob "
        f"--rw=write "
        f"--bs=2M "
        f"--numjobs={numjobs} "
        f"--iodepth=64 "
        f"--size={per_file_gib}G "
        f"--filename={fio_filename_str} "
        f"--ioengine=libaio "
        f"--group_reporting"
    )

    print(f"[INFO] Starte fio mit {numjobs} Jobs, je {per_file_gib} GiB...")
    process = subprocess.Popen(fio_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        for line in process.stdout:
            print(line.strip())
        process.wait()
    except KeyboardInterrupt:
        process.kill()
        print("[ABBRUCH] Füllen wurde manuell abgebrochen.")
        raise

    if process.returncode != 0:
        raise Exception("fio ist mit Fehlern beendet.")

def clear_fill():
    print("[INFO] Entferne Dummy-Dateien...")
    run_cmd(f"rm -f {MOUNTPOINT}/fillfile_*", check=False)

def simulate_resilver(pool_name, used_disks):
    failed_path = used_disks[0]

    print(f"[INFO] Nehme Disk offline: {failed_path}")
    run_cmd(f"zpool offline {pool_name} {failed_path}")
    time.sleep(0.5)

    print(f"[INFO] Wipe Disk {failed_path} (simulierter Replacement)...")
    run_cmd(f"wipefs -a {failed_path}", check=False)
    run_cmd(f"dd if=/dev/zero of={failed_path} bs=1M count=10", check=False)

    print(f"[INFO] Bringe Disk wieder online: {failed_path}")
    start_time = time.time()
    run_cmd(f"zpool online {pool_name} {failed_path}")

    print("[INFO] Warte auf Resilvering...")
    while True:
        status = run_cmd(f"zpool status {pool_name}", check=False)
        if "resilver" in status and "in progress" in status:
            time.sleep(1)
        else:
            break
    end_time = time.time()
    duration = end_time - start_time

    print(f"[INFO] Resilver abgeschlossen.")
    return duration, status

def delete_pool(pool_name):
    print("[INFO] Lösche Pool...")
    run_cmd("pkill -9 fio", check=False)
    run_cmd(f"fuser -k {MOUNTPOINT}", check=False)
    run_cmd(f"umount -f {MOUNTPOINT}", check=False)
    run_cmd(f"zpool destroy {pool_name}", check=False)

def main():
    dev_paths = get_valid_disk_paths()
    if len(dev_paths) < 5:
        print("[FEHLER] Nicht genug gültige Disks gefunden!")
        return

    configs = generate_rg_configs(dev_paths)
    timestamp = datetime.now().strftime("%Y%m%d")
    logfile = f"resilver_WorstCaseMitWipe_Fill:{FILL_LEVELS[0]}_{timestamp}.log"

    for i, cfg in enumerate(configs):
        print(f"\n[CONFIG {i+1}/{len(configs)}] {cfg['zfs_syntax']}")
        for level in FILL_LEVELS:
            for numjobs in NUMJOBS_LIST:
                try:
                    print(f"\n[TEST] {int(level*100)}% Füllstand | Numjobs: {numjobs}")
                    create_pool(cfg["zpool_create_cmd"], cfg["used_disks"])
                    fill_pool(level, numjobs)
                    duration, status = simulate_resilver(POOL_NAME, cfg["used_disks"])
                    clear_fill()
                    delete_pool(POOL_NAME)

                    with open(logfile, "a") as f:
                        f.write(f"--- Config: {cfg['zfs_syntax']} | Fill: {int(level*100)}% | Numjobs: {numjobs} ---\n")
                        f.write(f"VDEVs: {cfg['vdevs']}, Data: {cfg['data']}, Children: {cfg['children']}\n")
                        f.write(f"Resilver-Zeit: {duration:.2f} Sekunden\n")
                        f.write(status + "\n\n")

                except Exception as e:
                    print(f"[FEHLER] Test fehlgeschlagen: {e}")
                    try:
                        clear_fill()
                        delete_pool(POOL_NAME)
                    except:
                        pass
                    continue

    print(f"\n Tests abgeschlossen: {logfile}")

if __name__ == "__main__":
    try:
        tune_cache_for_benchmark()
        main()
    except KeyboardInterrupt:
        print("\n[ABBRUCH] Manuell gestoppt. Aufräumen...")
        try:
            clear_fill()
            delete_pool(POOL_NAME)
        except:
            print("[WARNUNG] Konnte nicht sauber aufräumen.")
    finally:
        restore_cache_settings()
