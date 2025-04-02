import subprocess
import time
from datetime import datetime

POOL_NAME = "mypool"
MOUNTPOINT = "/mnt/draidBenchmark"
FILL_LEVELS = [0, 0.2, 0.4] ########################### hier die prozente rein die wir testen wollen

def run_cmd(cmd, check=True):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f"[FEHLER] Befehl fehlgeschlagen: {cmd}")
        print(result.stderr)
        raise Exception("Fehler bei Kommandoausführung")
    return result.stdout.strip()

def get_valid_disk_paths():
    cmd = r'''
    for dev in /dev/sd*; do
        [[ "$dev" =~ [0-9] ]] && continue
        id=$(smartctl -i "$dev" 2>/dev/null | grep 'Logical Unit id' | awk '{print $4}')
        if [[ ${#id} -eq 18 ]]; then
            for prefix in /dev/disk/by-id/wwn-*; do
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
        print("Fehler beim Abrufen der Pfade:")
        print(result.stderr)
        return []
    lines = result.stdout.strip().splitlines()
    print(f"[INFO] {len(lines)} gültige Disks gefunden.")
    return lines

def generate_draid2_configs(dev_paths, min_children=4):
    total_disks = len(dev_paths) - 1
    configs = []

    for vdevs in range(1, total_disks + 1):
        if total_disks % vdevs != 0:
            continue

        children = total_disks // vdevs
        if children < min_children:
            continue

        spares = 1
        parity = 2
        data = children - parity - spares
        if data < 1:
            continue

        vdev_config = f"draid2:{data}d:{spares}s:{children}c"
        vdev_parts = []
        for i in range(vdevs):
            start = i * children
            end = start + children
            vdev_devs = " ".join(dev_paths[start:end])
            vdev_parts.append(f"{vdev_config} {vdev_devs}")

        zpool_cmd = (
            f"zpool create -f -m {MOUNTPOINT} -o ashift=12 {POOL_NAME} \\\n  " +
            " \\\n  ".join(vdev_parts)
        )

        configs.append({
            "vdevs": vdevs,
            "children": children,
            "spares": spares,
            "parity": parity,
            "data": data,
            "zfs_syntax": vdev_config,
            "zpool_create_cmd": zpool_cmd,
            "used_disks": dev_paths[:total_disks],
            "spare_disk": dev_paths[total_disks]
        })

    return configs

def create_pool(pool_cmd):
    print("[INFO] Erstelle Pool...")
    run_cmd(pool_cmd)
    print("[INFO] Deaktiviere Kompression...")
    run_cmd(f"zfs set compression=off {POOL_NAME}")

def fill_pool(level, num_vdevs):
    print(f"[INFO] Fülle Pool zu {int(level * 100)}% mit fio...")

    output = run_cmd(f"zfs list -Hp -o available {POOL_NAME}")
    available_bytes = int(output.strip())
    fill_size_bytes = int(available_bytes * level)
    fill_size_gib = fill_size_bytes // (1024 ** 3)

    print(f"[INFO] Zielgröße gesamt: {fill_size_gib} GiB")

    per_file_gib = max(1, fill_size_gib // num_vdevs)
    filenames = []

    for i in range(num_vdevs):
        filename = f"{MOUNTPOINT}/fillfile_{i}"
        filenames.append(filename)

    fio_filename_str = ":".join(filenames)

    fio_cmd = (
        f"fio --name=filljob "
        f"--rw=write "
        f"--bs=1M "
        f"--numjobs={num_vdevs} "
        f"--iodepth=64 "
        f"--size={per_file_gib}G "
        f"--filename={fio_filename_str} "
        f"--direct=1 "
        f"--ioengine=libaio "
        f"--group_reporting"
    )

    print(f"[INFO] Starte fio mit {num_vdevs} Jobs, je {per_file_gib} GiB...")
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

def simulate_resilver(pool_name, used_disks, spare_disk):
    failed_path = used_disks[0]
    replacement_path = spare_disk

    print(f"[INFO] Nehme Disk offline: {failed_path}")
    run_cmd(f"zpool offline {pool_name} {failed_path}")
    time.sleep(1)

    print(f"[INFO] Ersetze durch Ersatz-Disk: {replacement_path}")
    run_cmd(f"zpool replace {pool_name} {failed_path} {replacement_path}")
    time.sleep(2)

    print("[INFO] Warte auf Resilvering...")
    start_time = time.time()
    while True:
        status = run_cmd(f"zpool status {pool_name}", check=False)
        if "resilver in progress" in status:
            time.sleep(2)
        else:
            break
    end_time = time.time()
    duration = end_time - start_time

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
        print("Nicht genug gültige Disks gefunden!")
        return

    configs = generate_draid2_configs(dev_paths)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    logfile = f"resilver_tests_{timestamp}.log"

    for i, cfg in enumerate(configs):
        print(f"\n[CONFIG {i+1}/{len(configs)}] {cfg['zfs_syntax']}")
        for level in FILL_LEVELS:
            try:
                print(f"\n[TEST] {int(level*100)}% Füllstand")
                create_pool(cfg["zpool_create_cmd"])
                fill_pool(level, cfg["vdevs"])
                duration, status = simulate_resilver(POOL_NAME, cfg["used_disks"], cfg["spare_disk"])
                clear_fill()
                delete_pool(POOL_NAME)

                with open(logfile, "a") as f:
                    f.write(f"--- Konfiguration: {cfg['zfs_syntax']} | Fill: {int(level*100)}% ---\n")
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

    print(f"\n✅ Alle Tests abgeschlossen. Ergebnisse gespeichert in: {logfile}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[ABBRUCH] Manuell gestoppt. Aufräumen...")
        try:
            clear_fill()
            delete_pool(POOL_NAME)
        except:
            print("[WARNUNG] Konnte nicht sauber aufräumen.")
