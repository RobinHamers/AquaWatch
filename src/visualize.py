"""Map and chart generation — Weekend 4."""

import logging
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.gridspec as gridspec
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rasterio

logger = logging.getLogger(__name__)

SEVERITY_COLORS = {"LOW": "#FFC107", "MEDIUM": "#FF7043", "HIGH": "#D32F2F"}
NDCI_VMIN, NDCI_VMAX = -0.1, 0.5


def _add_scalebar(ax, x_span_m: float) -> None:
    xlim, ylim = ax.get_xlim(), ax.get_ylim()
    bar_m = round(x_span_m * 0.15 / 1000) * 1000
    bar_m = max(bar_m, 500)
    x0 = xlim[0] + 0.05 * (xlim[1] - xlim[0])
    y0 = ylim[0] + 0.05 * (ylim[1] - ylim[0])
    dy = (ylim[1] - ylim[0]) * 0.012
    ax.plot([x0, x0 + bar_m], [y0, y0], color="black", lw=3, solid_capstyle="butt", zorder=6)
    ax.text(x0 + bar_m / 2, y0 + dy, f"{bar_m//1000:.0f} km", ha="center", va="bottom",
            fontsize=8, zorder=6)


def _add_north_arrow(ax) -> None:
    xlim, ylim = ax.get_xlim(), ax.get_ylim()
    x = xlim[1] - 0.07 * (xlim[1] - xlim[0])
    y_tail = ylim[1] - 0.12 * (ylim[1] - ylim[0])
    y_head = ylim[1] - 0.05 * (ylim[1] - ylim[0])
    ax.annotate(
        "N", xy=(x, y_head), xytext=(x, y_tail),
        arrowprops=dict(arrowstyle="-|>", color="black", lw=1.5),
        ha="center", va="center", fontsize=9, fontweight="bold", zorder=6,
    )


def plot_alert_map(
    alert,
    ndci_path: Path,
    reservoir_geojson: Path,
    output_path: Path,
) -> Path:
    """Generate a spatial NDCI map for one alert event.

    Shows NDCI raster with diverging colormap, reservoir boundary,
    severity badge, scale bar, north arrow, and colorbar.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    import geopandas as gpd
    with rasterio.open(ndci_path) as src:
        ndci = src.read(1, masked=True).astype(np.float32)
        bounds = src.bounds
        raster_crs = src.crs

    gdf = gpd.read_file(reservoir_geojson).to_crs(raster_crs)

    ndci_display = np.where(ndci.mask, np.nan, ndci.data)
    extent = [bounds.left, bounds.right, bounds.bottom, bounds.top]

    fig, ax = plt.subplots(figsize=(10, 9))
    img = ax.imshow(ndci_display, cmap="RdBu_r", vmin=NDCI_VMIN, vmax=NDCI_VMAX,
                    extent=extent, origin="upper", interpolation="nearest")
    gdf.boundary.plot(ax=ax, color="#222222", linewidth=1.5, zorder=5)

    cbar = plt.colorbar(img, ax=ax, shrink=0.72, pad=0.02)
    cbar.set_label("NDCI (Cyanobacteria Index)", fontsize=10)
    cbar.ax.tick_params(labelsize=8)

    badge_color = SEVERITY_COLORS.get(alert.severity, "#888888")
    badge = mpatches.FancyBboxPatch(
        (0.02, 0.915), 0.22, 0.065, transform=ax.transAxes,
        boxstyle="round,pad=0.01", facecolor=badge_color,
        edgecolor="white", linewidth=1.5, zorder=10,
    )
    ax.add_patch(badge)
    ax.text(0.13, 0.948, alert.severity, transform=ax.transAxes,
            ha="center", va="center", fontsize=11, fontweight="bold",
            color="white", zorder=11)

    ax.set_title(
        f"AquaWatch — Serre-Ponçon  |  {alert.date}  |  "
        f"NDCI={alert.ndci_mean:.4f}  z={alert.z_score:.2f}",
        fontsize=11, pad=8,
    )
    ax.set_xlabel("Easting (m)", fontsize=9)
    ax.set_ylabel("Northing (m)", fontsize=9)
    ax.tick_params(labelsize=8)

    _add_scalebar(ax, bounds.right - bounds.left)
    _add_north_arrow(ax)

    plt.tight_layout()
    fig.savefig(str(output_path), dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Saved alert map → %s", output_path)
    return output_path


def plot_bloom_comparison(
    dates: list,
    ndci_paths: list[Path],
    reservoir_geojson: Path,
    output_path: Path,
    title: str = "Bloom Event Progression",
) -> Path:
    """Multi-panel NDCI map for a sequence of dates (before/during/after)."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    n = len(dates)
    fig, axes = plt.subplots(1, n, figsize=(5 * n, 6), constrained_layout=True)
    if n == 1:
        axes = [axes]

    fig.suptitle(title, fontsize=13)
    last_img = None

    import geopandas as gpd
    for i, (d, path, ax) in enumerate(zip(dates, ndci_paths, axes)):
        with rasterio.open(path) as src:
            ndci = src.read(1, masked=True).astype(np.float32)
            bounds = src.bounds
            raster_crs = src.crs

        gdf = gpd.read_file(reservoir_geojson).to_crs(raster_crs)
        ndci_display = np.where(ndci.mask, np.nan, ndci.data)
        extent = [bounds.left, bounds.right, bounds.bottom, bounds.top]

        img = ax.imshow(ndci_display, cmap="RdBu_r", vmin=NDCI_VMIN, vmax=NDCI_VMAX,
                        extent=extent, origin="upper", interpolation="nearest")
        gdf.boundary.plot(ax=ax, color="#222222", linewidth=1.2)

        # Mean NDCI for subtitle
        valid = ndci_display[np.isfinite(ndci_display)]
        subtitle = f"{d}"
        if valid.size > 0:
            subtitle += f"\nNDCI={valid.mean():.4f}"
        ax.set_title(subtitle, fontsize=10)
        ax.tick_params(labelsize=7)
        ax.set_xlabel("Easting (m)", fontsize=8)
        if i == 0:
            ax.set_ylabel("Northing (m)", fontsize=8)
        else:
            ax.set_yticklabels([])
        last_img = img

    if last_img is not None:
        cbar = fig.colorbar(last_img, ax=axes, shrink=0.75, location="right")
        cbar.set_label("NDCI", fontsize=10)

    fig.savefig(str(output_path), dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Saved bloom comparison → %s", output_path)
    return output_path


def plot_dashboard(
    df: pd.DataFrame,
    alerts: list,
    output_path: Path,
) -> Path:
    """Single-page AquaWatch dashboard PNG.

    Layout: left column = NDCI + turbidity time series (top/bottom),
    right column = monthly alert bar chart (full height).
    """
    from alerts import compute_rolling_baseline  # avoid module-level circular

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    df_base = compute_rolling_baseline(df.set_index("date")).reset_index()

    fig = plt.figure(figsize=(16, 10))
    fig.suptitle(
        "AquaWatch — Lac de Serre-Ponçon Water Quality Monitor",
        fontsize=14, fontweight="bold", y=0.98,
    )

    gs = gridspec.GridSpec(2, 2, width_ratios=[3, 1], height_ratios=[1, 1],
                           hspace=0.38, wspace=0.28)
    ax_ndci = fig.add_subplot(gs[0, 0])
    ax_turb = fig.add_subplot(gs[1, 0], sharex=ax_ndci)
    ax_bar  = fig.add_subplot(gs[:, 1])

    # ── NDCI time series ─────────────────────────────────────────────────────
    bm = df_base["ndci_baseline_mean"]
    bs = df_base["ndci_baseline_std"].fillna(0)
    ax_ndci.fill_between(df_base["date"], bm - bs, bm + bs,
                         alpha=0.15, color="#2166ac", label="±1σ baseline")
    ax_ndci.plot(df_base["date"], bm, "--", color="#888888",
                 linewidth=1.0, alpha=0.7, label="30-day baseline")
    ax_ndci.plot(df["date"], df["ndci_water_mean"], "o-",
                 color="#2166ac", linewidth=1.5, markersize=3, label="NDCI mean")

    # Alert markers
    plotted_sev = set()
    for alert in sorted(alerts, key=lambda a: a.date):
        color = SEVERITY_COLORS.get(alert.severity, "#888888")
        match = df[df["date"].dt.date == alert.date]
        y_val = match["ndci_water_mean"].iloc[0] if not match.empty else None
        if y_val is not None:
            lbl = alert.severity if alert.severity not in plotted_sev else "_"
            ax_ndci.plot(pd.Timestamp(alert.date), y_val, "^", color=color,
                         markersize=10, zorder=5, label=lbl)
            plotted_sev.add(alert.severity)
        ax_ndci.axvline(pd.Timestamp(alert.date), color=color,
                        alpha=0.3, linewidth=1.0, linestyle=":")

    ax_ndci.set_ylabel("NDCI (water pixels)", fontsize=9)
    ax_ndci.legend(fontsize=7, ncol=4, loc="upper left")
    ax_ndci.grid(axis="y", linestyle=":", alpha=0.4)
    ax_ndci.set_title("NDCI Time Series with Alert Markers", fontsize=10)
    plt.setp(ax_ndci.get_xticklabels(), visible=False)

    # ── Turbidity time series ─────────────────────────────────────────────────
    ax_turb.fill_between(df["date"],
                         df["turbidity_water_p25"], df["turbidity_water_p75"],
                         alpha=0.2, color="#762a83", label="IQR")
    ax_turb.plot(df["date"], df["turbidity_water_mean"], "o-",
                 color="#762a83", linewidth=1.5, markersize=3, label="Turbidity mean")
    ax_turb.set_ylabel("Turbidity (B04/B03)", fontsize=9)
    ax_turb.set_title("Turbidity Time Series", fontsize=10)
    ax_turb.legend(fontsize=7, loc="upper left")
    ax_turb.grid(axis="y", linestyle=":", alpha=0.4)
    ax_turb.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax_turb.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    fig.autofmt_xdate(rotation=30)

    # ── Monthly alert bar chart ───────────────────────────────────────────────
    sev_rank = {"LOW": 1, "MEDIUM": 2, "HIGH": 3}
    alert_months = pd.to_datetime([str(a.date) for a in alerts]).to_period("M")
    month_counts = {}
    month_worst = {}
    for a, m in zip(alerts, alert_months):
        month_counts[m] = month_counts.get(m, 0) + 1
        if m not in month_worst or sev_rank[a.severity] > sev_rank[month_worst[m]]:
            month_worst[m] = a.severity

    if month_counts:
        sorted_months = sorted(month_counts)
        labels = [str(m) for m in sorted_months]
        counts = [month_counts[m] for m in sorted_months]
        colors = [SEVERITY_COLORS[month_worst[m]] for m in sorted_months]
        ax_bar.barh(range(len(sorted_months)), counts, color=colors,
                    edgecolor="white", height=0.7)
        ax_bar.set_yticks(range(len(sorted_months)))
        ax_bar.set_yticklabels(labels, fontsize=8)
        ax_bar.set_xlabel("Alert count", fontsize=9)
        ax_bar.xaxis.set_major_locator(plt.MaxNLocator(integer=True))
        ax_bar.grid(axis="x", linestyle=":", alpha=0.4)

    ax_bar.set_title("Alerts by Month", fontsize=10)
    patches = [mpatches.Patch(color=c, label=s) for s, c in SEVERITY_COLORS.items()]
    ax_bar.legend(handles=patches, fontsize=8, loc="lower right")

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    fig.savefig(str(output_path), dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Saved dashboard → %s", output_path)
    return output_path


def plot_fused_dashboard(
    s2_df: pd.DataFrame,
    s3_df: pd.DataFrame,
    alerts: list,
    precursor_events: list,
    output_path: Path,
) -> Path:
    """S2 + S3 fusion dashboard PNG.

    Layout:
    - Top panel  : NDCI time series (S2 blue, S3 green, alert markers, precursor markers)
    - Middle panel: S3 daily coverage (available/missing as a rug plot)
    - Bottom panel: S2 turbidity with alert severity annotations

    Parameters
    ----------
    s2_df : S2 timeseries DataFrame
    s3_df : S3 timeseries DataFrame
    alerts : list of Alert objects from S2 detection
    precursor_events : list of PrecursorEvent from fusion.detect_s3_precursor_alerts()
    output_path : destination PNG path
    """
    from alerts import compute_rolling_baseline  # avoid circular import

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    s2 = s2_df.copy()
    s3 = s3_df.copy()
    s2["date"] = pd.to_datetime(s2["date"])
    s3["date"] = pd.to_datetime(s3["date"])
    s2 = s2.sort_values("date")
    s3 = s3.sort_values("date")

    fig, axes = plt.subplots(3, 1, figsize=(16, 12), sharex=True,
                             gridspec_kw={"height_ratios": [3, 1, 2]})
    fig.suptitle(
        "AquaWatch — S2 × S3 Fusion Dashboard  |  Lac de Serre-Ponçon",
        fontsize=14, fontweight="bold", y=0.99,
    )

    ax_ndci, ax_cov, ax_turb = axes

    # ── Panel 1: NDCI time series ─────────────────────────────────────────────
    s2_base = compute_rolling_baseline(s2.set_index("date")).reset_index()
    bm = s2_base["ndci_baseline_mean"]
    bs = s2_base["ndci_baseline_std"].fillna(0)
    ax_ndci.fill_between(s2_base["date"], bm - bs, bm + bs,
                         alpha=0.12, color="#2166ac", label="S2 ±1σ baseline")

    # S2 NDCI
    ax_ndci.plot(s2["date"], s2["ndci_water_mean"], "o-",
                 color="#2166ac", linewidth=1.5, markersize=4, label="S2 NDCI (10 m)")

    # S3 NDCI
    ax_ndci.plot(s3["date"], s3["ndci_water_mean"], ".",
                 color="#1a9641", linewidth=0, markersize=3, alpha=0.7, label="S3 NDCI (300 m)")
    s3_smooth = s3.set_index("date")["ndci_water_mean"].rolling("7D").mean()
    ax_ndci.plot(s3_smooth.index, s3_smooth.values, "-",
                 color="#1a9641", linewidth=1.2, alpha=0.8, label="S3 7-day rolling mean")

    # S2 alert markers
    plotted_sev: set[str] = set()
    for alert in sorted(alerts, key=lambda a: a.date):
        color = SEVERITY_COLORS.get(alert.severity, "#888888")
        match = s2[s2["date"].dt.date == alert.date]
        y_val = match["ndci_water_mean"].iloc[0] if not match.empty else None
        if y_val is not None:
            lbl = f"S2 {alert.severity}" if alert.severity not in plotted_sev else "_"
            ax_ndci.plot(pd.Timestamp(alert.date), y_val, "^",
                         color=color, markersize=10, zorder=6, label=lbl)
            plotted_sev.add(alert.severity)

    # S3 precursor markers — star at first S3 detection
    for ev in precursor_events:
        if ev.s3_first_date is None:
            continue
        s3_match = s3[s3["date"].dt.date == ev.s3_first_date]
        y_s3 = s3_match["ndci_water_mean"].iloc[0] if not s3_match.empty else NDCI_LOW
        ax_ndci.plot(pd.Timestamp(ev.s3_first_date), y_s3, "*",
                     color="#1a9641", markersize=14, zorder=7,
                     label=f"S3 precursor ({ev.bloom_label})")
        if ev.precursor_days and ev.precursor_days > 0:
            ax_ndci.annotate(
                f"−{ev.precursor_days}d",
                xy=(pd.Timestamp(ev.s3_first_date), y_s3),
                xytext=(10, 12), textcoords="offset points",
                fontsize=8, color="#1a9641", fontweight="bold",
            )

    # Threshold lines
    NDCI_LOW_V, NDCI_MED_V, NDCI_HIGH_V = 0.2, 0.3, 0.4
    ax_ndci.axhline(NDCI_LOW_V,  color="#fdae61", linewidth=1.0, linestyle="--", alpha=0.8)
    ax_ndci.axhline(NDCI_MED_V,  color="#f46d43", linewidth=1.0, linestyle="--", alpha=0.8)
    ax_ndci.axhline(NDCI_HIGH_V, color="#d73027", linewidth=1.0, linestyle="--", alpha=0.8)

    ax_ndci.set_ylabel("NDCI", fontsize=9)
    ax_ndci.legend(fontsize=7, ncol=4, loc="upper left")
    ax_ndci.grid(axis="y", linestyle=":", alpha=0.4)
    ax_ndci.set_title("NDCI Time Series — S2 (blue) vs S3 (green)", fontsize=10)

    # Summer shading on all panels
    for year in (2023, 2024):
        for ax in axes:
            ax.axvspan(pd.Timestamp(f"{year}-06-01"), pd.Timestamp(f"{year}-09-30"),
                       alpha=0.06, color="orange")

    # ── Panel 2: S3 daily coverage ────────────────────────────────────────────
    s3_dates = s3["date"]
    ax_cov.eventplot(
        [mdates.date2num(d) for d in s3_dates],
        orientation="horizontal", lineoffsets=0.5, linelengths=0.9,
        linewidths=0.8, color="#1a9641", alpha=0.6,
    )
    ax_cov.set_ylabel("S3\ndays", fontsize=8)
    ax_cov.set_yticks([])
    ax_cov.set_title("S3 Daily Coverage (clear-sky scenes)", fontsize=9)
    ax_cov.grid(False)

    # ── Panel 3: S2 turbidity ─────────────────────────────────────────────────
    if "turbidity_water_mean" in s2.columns:
        ax_turb.fill_between(s2["date"],
                             s2.get("turbidity_water_p25", np.nan),
                             s2.get("turbidity_water_p75", np.nan),
                             alpha=0.2, color="#762a83", label="IQR")
        ax_turb.plot(s2["date"], s2["turbidity_water_mean"], "o-",
                     color="#762a83", linewidth=1.5, markersize=3, label="S2 turbidity")
    ax_turb.set_ylabel("Turbidity (B04/B03)", fontsize=9)
    ax_turb.legend(fontsize=7, loc="upper left")
    ax_turb.grid(axis="y", linestyle=":", alpha=0.4)
    ax_turb.set_title("S2 Turbidity Time Series", fontsize=10)
    ax_turb.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax_turb.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    fig.autofmt_xdate(rotation=30)

    plt.tight_layout(rect=[0, 0, 1, 0.98])
    fig.savefig(str(output_path), dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Saved fused dashboard → %s", output_path)
    return output_path


def plot_comparison_dashboard(
    reservoirs: dict,
    output_path: Path,
) -> Path:
    """Side-by-side multi-reservoir NDCI time series dashboard (Figure 1 for ES4S paper).

    One row per reservoir. Each row shows the NDCI time series, rolling baseline,
    alert markers, and known bloom period shading. Separate x-axes (periods differ).

    Parameters
    ----------
    reservoirs : mapping of reservoir_key → dict with keys:
                   timeseries (DataFrame), alerts (list[Alert]), config (dict)
    output_path : destination PNG

    Returns
    -------
    Path to saved PNG.
    """
    from alerts import compute_rolling_baseline

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    res_items = list(reservoirs.items())
    n = len(res_items)

    fig, axes = plt.subplots(n, 1, figsize=(16, 5 * n))
    if n == 1:
        axes = [axes]
    fig.suptitle(
        "AquaWatch — Multi-Reservoir Validation  |  Same thresholds, no retuning",
        fontsize=14, fontweight="bold", y=1.01,
    )

    row_colors = ["#2166ac", "#d6604d", "#4dac26", "#762a83"]

    for idx, (res_name, res_data) in enumerate(res_items):
        ax  = axes[idx]
        df  = res_data["timeseries"].copy()
        al  = res_data["alerts"]
        cfg = res_data["config"]
        color = row_colors[idx % len(row_colors)]

        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")

        df_base = compute_rolling_baseline(df.set_index("date")).reset_index()
        bm = df_base["ndci_baseline_mean"]
        bs = df_base["ndci_baseline_std"].fillna(0)

        ax.fill_between(df_base["date"], bm - bs, bm + bs,
                        alpha=0.12, color=color, label="±1σ baseline")
        ax.plot(df_base["date"], bm, "--", color="#888888",
                linewidth=0.9, alpha=0.6, label="30-day baseline")
        ax.plot(df["date"], df["ndci_water_mean"], "o-",
                color=color, linewidth=1.5, markersize=3.5, label="NDCI mean")

        # Bloom period shading
        for bloom in cfg.get("known_blooms", []):
            ax.axvspan(
                pd.Timestamp(bloom["start"]), pd.Timestamp(bloom["end"]),
                alpha=0.10, color="gold",
                label=f"Known bloom ({bloom['label']})" if bloom == cfg["known_blooms"][0] else "_",
            )

        # Alert markers
        plotted_sev: set[str] = set()
        for alert in sorted(al, key=lambda a: a.date):
            ac = SEVERITY_COLORS.get(alert.severity, "#888888")
            match = df[df["date"].dt.date == alert.date]
            y_val = match["ndci_water_mean"].iloc[0] if not match.empty else None
            if y_val is not None:
                lbl = alert.severity if alert.severity not in plotted_sev else "_"
                ax.plot(pd.Timestamp(alert.date), y_val, "^",
                        color=ac, markersize=9, zorder=6, label=lbl)
                plotted_sev.add(alert.severity)

        # Threshold lines (only labelled on first row)
        for val, col, lab in [
            (0.2, "#fdae61", "LOW (0.20)"),
            (0.3, "#f46d43", "MED (0.30)"),
            (0.4, "#d73027", "HIGH (0.40)"),
        ]:
            ax.axhline(val, color=col, linewidth=0.9, linestyle="--", alpha=0.8,
                       label=lab if idx == 0 else "_")

        country = cfg.get("country", "")
        area    = cfg.get("area_km2", "?")
        ax.set_title(
            f"{cfg.get('name', res_name)}  [{country}  ·  {area} km²  ·  EPSG:{cfg.get('epsg','?').split(':')[-1]}]",
            fontsize=11, loc="left",
        )
        ax.set_ylabel("NDCI (water)", fontsize=9)
        ax.legend(fontsize=7, ncol=5, loc="upper left")
        ax.grid(axis="y", linestyle=":", alpha=0.35)
        ax.set_ylim(-0.15, 0.60)

        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        fig.autofmt_xdate(rotation=25)

    plt.tight_layout()
    fig.savefig(str(output_path), dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Saved comparison dashboard → %s", output_path)
    return output_path
