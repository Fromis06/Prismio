package cli

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"my-cdc/internal/capture"
	"my-cdc/internal/config"
	"my-cdc/internal/sinks"

	"github.com/atotto/clipboard"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type configRow struct {
	Label         string
	Get           func() string
	Validate      func(newVal string) error
	Set           func(newVal string)
	IsAction      bool
	OnAction      func()
	IsCheckStatus bool               // True for connection-check rows.
	StatusColor   func() tcell.Color // Value color for a check row.
}

// checkState stays around when the table rebuilds.
type checkState struct {
	status string // "unchecked" | "checking" | "ok" | "failed"
	errMsg string
}

func statusText(cs *checkState) string {
	switch cs.status {
	case "checking":
		return "◐ CHECKING..."
	case "ok":
		return "● OK"
	case "failed":
		return fmt.Sprintf("● ERROR: %s", cs.errMsg)
	default:
		return "● NOT CHECKED"
	}
}

func statusColor(cs *checkState) tcell.Color {
	switch cs.status {
	case "checking":
		return tcell.ColorYellow
	case "ok":
		return tcell.ColorGreen
	default: // "unchecked" or "failed"
		return tcell.ColorRed
	}
}

func NewConfigForm(tuiApp *tview.Application, cfg *config.AppConfig, configPath string, runCallback func()) (tview.Primitive, func()) {
	table := tview.NewTable().SetSelectable(true, false)
	table.SetBorder(true).SetTitle(" Configuration ")

	statusView := tview.NewTextView().SetDynamicColors(true)

	locked := false
	var rows []configRow

	// One check state for source and every destination.
	sourceCheck := &checkState{status: "unchecked"}
	consumerChecks := make([]*checkState, len(cfg.Consumers.List))
	for i := range consumerChecks {
		consumerChecks[i] = &checkState{status: "unchecked"}
	}

	var buildRows func()
	var rebuildTable func()
	var startEdit func(rowIdx int)
	var showAddSinkTypeDropdown func()
	var showAddSourceTypeDropdown func()
	var runCheck func(cs *checkState, testFn func(ctx context.Context) error)

	// Save config after edits.
	persist := func() {
		if saveErr := config.SaveFullConfig(configPath, cfg); saveErr != nil {
			statusView.SetText(fmt.Sprintf("[red]Failed to save config: %v[-]", saveErr))
		}
	}

	// Checks run away from the UI thread. Rebuild avoids stale row indexes.
	runCheck = func(cs *checkState, testFn func(ctx context.Context) error) {
		if cs.status == "checking" {
			return
		}
		cs.status = "checking"
		cs.errMsg = ""
		selRow, selCol := table.GetSelection()
		rebuildTable()
		table.Select(selRow, selCol)

		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			err := testFn(ctx)
			tuiApp.QueueUpdateDraw(func() {
				if err != nil {
					cs.status = "failed"
					cs.errMsg = err.Error()
				} else {
					cs.status = "ok"
					cs.errMsg = ""
				}
				r, c := table.GetSelection()
				rebuildTable()
				table.Select(r, c)
			})
		}()
	}

	buildRows = func() {
		var newRows []configRow

		// Data source.
		if cfg.Provider.Source.Type == "" {
			newRows = append(newRows, configRow{
				Label:    "     →  Choose data source",
				IsAction: true,
				Get:      func() string { return "" },
				OnAction: func() {
					if locked {
						return
					}
					showAddSourceTypeDropdown()
				},
			})
		} else {
			newRows = append(newRows, configRow{
				Label: fmt.Sprintf("Source URL (%s)", cfg.Provider.Source.Type),
				Get:   func() string { return cfg.Provider.Source.URL },
				Set: func(v string) {
					cfg.Provider.Source.URL = v
					sourceCheck.status = "unchecked"
					sourceCheck.errMsg = ""
				},
			})
			newRows = append(newRows, configRow{
				Label:         "     →  Check source connection",
				IsAction:      true,
				IsCheckStatus: true,
				StatusColor:   func() tcell.Color { return statusColor(sourceCheck) },
				Get:           func() string { return statusText(sourceCheck) },
				OnAction: func() {
					if locked {
						return
					}
					srcType := cfg.Provider.Source.Type
					srcURL := cfg.Provider.Source.URL
					runCheck(sourceCheck, func(ctx context.Context) error {
						return capture.TestConnection(ctx, srcType, srcURL)
					})
				},
			})
			newRows = append(newRows, configRow{
				Label:    "     →  Change data source",
				IsAction: true,
				Get:      func() string { return "" },
				OnAction: func() {
					if locked {
						return
					}
					cfg.Provider.Source.Type = ""
					cfg.Provider.Source.URL = ""
					cfg.Provider.Source.Name = ""
					sourceCheck.status = "unchecked"
					sourceCheck.errMsg = ""
					persist()
					rebuildTable()
					statusView.SetText("[yellow]Data source deselected[-]")
				},
			})
		}

		// Destinations.
		for i := range cfg.Consumers.List {
			idx := i
			label := fmt.Sprintf("Destination URL %d (%s)", idx+1, cfg.Consumers.List[idx].Type)
			deleteLabel := fmt.Sprintf("     →  Delete destination %d", idx+1)
			checkLabel := fmt.Sprintf("     →  Check destination connection %d", idx+1)

			newRows = append(newRows, configRow{
				Label: label,
				Get:   func() string { return cfg.Consumers.List[idx].URL },
				Set: func(v string) {
					cfg.Consumers.List[idx].URL = v
					if idx < len(consumerChecks) {
						consumerChecks[idx].status = "unchecked"
						consumerChecks[idx].errMsg = ""
					}
				},
			})
			newRows = append(newRows, configRow{
				Label:         checkLabel,
				IsAction:      true,
				IsCheckStatus: true,
				StatusColor:   func() tcell.Color { return statusColor(consumerChecks[idx]) },
				Get:           func() string { return statusText(consumerChecks[idx]) },
				OnAction: func() {
					if locked {
						return
					}
					sinkType := cfg.Consumers.List[idx].Type
					sinkURL := cfg.Consumers.List[idx].URL
					runCheck(consumerChecks[idx], func(ctx context.Context) error {
						return sinks.TestConnection(ctx, sinkType, sinkURL)
					})
				},
			})
			newRows = append(newRows, configRow{
				Label:    deleteLabel,
				IsAction: true,
				Get:      func() string { return "" },
				OnAction: func() {
					if locked {
						return
					}
					cfg.Consumers.List = append(cfg.Consumers.List[:idx], cfg.Consumers.List[idx+1:]...)
					consumerChecks = append(consumerChecks[:idx], consumerChecks[idx+1:]...)
					persist()
					rebuildTable()
					statusView.SetText(fmt.Sprintf("[yellow]Destination %d deleted[-]", idx+1))
				},
			})
		}

		newRows = append(newRows, configRow{
			Label:    "     →  Add new destination",
			IsAction: true,
			Get:      func() string { return "" },
			OnAction: func() {
				if locked {
					return
				}
				showAddSinkTypeDropdown()
			},
		})

		// These values only gets changed by AutoTuner in automatic mode.
		newRows = append(newRows,
			configRow{
				Label: "Worker Count",
				Get:   func() string { return strconv.Itoa(int(cfg.DataProcessing.DataProcessingWorkerCount.Load())) },
				Validate: func(v string) error {
					n, err := strconv.ParseInt(v, 10, 32)
					if err != nil || n <= 0 {
						return fmt.Errorf("Worker Count must be a positive integer")
					}
					return nil
				},
				Set: func(v string) {
					n, _ := strconv.ParseInt(v, 10, 32)
					cfg.DataProcessing.DataProcessingWorkerCount.Store(int32(n))
				},
			},
			configRow{
				Label: "Batch Size",
				Get:   func() string { return strconv.FormatInt(cfg.Batch.BatchMaxSize.Load(), 10) },
				Validate: func(v string) error {
					n, err := strconv.ParseInt(v, 10, 64)
					if err != nil || n <= 0 {
						return fmt.Errorf("Batch Size must be a positive integer")
					}
					return nil
				},
				Set: func(v string) {
					n, _ := strconv.ParseInt(v, 10, 64)
					cfg.Batch.BatchMaxSize.Store(n)
				},
			},
			configRow{
				Label: "Batch Timeout (ms)",
				Get:   func() string { return strconv.FormatInt(cfg.Batch.BatchTimeout.Load(), 10) },
				Validate: func(v string) error {
					n, err := strconv.ParseInt(v, 10, 64)
					if err != nil || n <= 0 {
						return fmt.Errorf("Batch Timeout must be a positive integer")
					}
					return nil
				},
				Set: func(v string) {
					n, _ := strconv.ParseInt(v, 10, 64)
					cfg.Batch.BatchTimeout.Store(n)
				},
			},
			configRow{
				Label: "Flush Timeout (ms)",
				Get:   func() string { return strconv.FormatInt(cfg.Batch.FlushTimeoutMs.Load(), 10) },
				Validate: func(v string) error {
					n, err := strconv.ParseInt(v, 10, 64)
					if err != nil || n <= 0 {
						return fmt.Errorf("Flush Timeout must be a positive integer")
					}
					return nil
				},
				Set: func(v string) {
					n, _ := strconv.ParseInt(v, 10, 64)
					cfg.Batch.FlushTimeoutMs.Store(n)
				},
			},
			configRow{
				Label: "Bag Max Size",
				Get:   func() string { return strconv.FormatInt(cfg.Bag.BagMaxSize.Load(), 10) },
				Validate: func(v string) error {
					n, err := strconv.ParseInt(v, 10, 64)
					if err != nil || n <= 0 {
						return fmt.Errorf("Bag Max Size must be a positive integer")
					}
					return nil
				},
				Set: func(v string) {
					n, _ := strconv.ParseInt(v, 10, 64)
					cfg.Bag.BagMaxSize.Store(n)
				},
			},
			configRow{
				Label: "Bag Max Multiple",
				Get:   func() string { return strconv.Itoa(int(cfg.Bag.BagMaxMultiple.Load())) },
				Validate: func(v string) error {
					n, err := strconv.ParseInt(v, 10, 32)
					if err != nil || n <= 0 {
						return fmt.Errorf("Bag Max Multiple must be a positive integer")
					}
					return nil
				},
				Set: func(v string) {
					n, _ := strconv.ParseInt(v, 10, 32)
					cfg.Bag.BagMaxMultiple.Store(int32(n))
				},
			},
			configRow{
				Label: "Feedback Interval (s)",
				Get:   func() string { return strconv.Itoa(int(cfg.Capture.FeedbackInterval.Load())) },
				Validate: func(v string) error {
					n, err := strconv.ParseInt(v, 10, 32)
					if err != nil || n <= 0 {
						return fmt.Errorf("Feedback Interval must be a positive integer")
					}
					return nil
				},
				Set: func(v string) {
					n, _ := strconv.ParseInt(v, 10, 32)
					cfg.Capture.FeedbackInterval.Store(int32(n))
				},
			},
			configRow{
				Label: "Pipeline Max Size",
				Get:   func() string { return strconv.Itoa(int(cfg.Pipeline.PipelineMaxSize.Load())) },
				Validate: func(v string) error {
					n, err := strconv.ParseInt(v, 10, 32)
					if err != nil || n <= 0 {
						return fmt.Errorf("Pipeline Max Size must be a positive integer")
					}
					return nil
				},
				Set: func(v string) {
					n, _ := strconv.ParseInt(v, 10, 32)
					cfg.Pipeline.PipelineMaxSize.Store(int32(n))
				},
			},
		)

		rows = newRows
	}

	redrawRow := func(i int) {
		r := rows[i]
		labelColor := tcell.ColorYellow
		valueText := r.Get()
		valueColor := tcell.ColorWhite
		if r.IsAction {
			labelColor = tcell.ColorAqua
			if r.IsCheckStatus {
				valueColor = r.StatusColor()
			} else {
				valueText = "(Enter / double-click)"
			}
		}
		table.SetCell(i, 0, tview.NewTableCell(r.Label).
			SetTextColor(labelColor).
			SetSelectable(true).
			SetExpansion(1))
		table.SetCell(i, 1, tview.NewTableCell(valueText).
			SetTextColor(valueColor).
			SetSelectable(true).
			SetExpansion(4))
	}

	rebuildTable = func() {
		buildRows()
		table.Clear()
		for i := range rows {
			redrawRow(i)
		}
	}

	copySelected := func() {
		row, _ := table.GetSelection()
		if row < 0 || row >= len(rows) || rows[row].IsAction {
			statusView.SetText("[yellow]This row has no value to copy[-]")
			return
		}
		val := rows[row].Get()
		if err := clipboard.WriteAll(val); err != nil {
			statusView.SetText(fmt.Sprintf("[red]Copy failed: %v[-]", err))
			return
		}
		statusView.SetText(fmt.Sprintf("[green]Copied: %s[-]", rows[row].Label))
	}

	rootPages := tview.NewPages()

	startEdit = func(rowIdx int) {
		if rowIdx < 0 || rowIdx >= len(rows) {
			return
		}
		r := rows[rowIdx]
		if r.IsAction {
			r.OnAction()
			return
		}
		if locked {
			return
		}

		input := tview.NewInputField().
			SetLabel(r.Label + ": ").
			SetText(r.Get()).
			SetFieldWidth(0)

		closeEdit := func() {
			rootPages.RemovePage("edit")
			tuiApp.SetFocus(table)
		}

		input.SetDoneFunc(func(key tcell.Key) {
			if key == tcell.KeyEnter {
				newVal := input.GetText()
				if r.Validate != nil {
					if err := r.Validate(newVal); err != nil {
						statusView.SetText(fmt.Sprintf("[red]%v[-]", err))
						return
					}
				}
				r.Set(newVal)
				rebuildTable()
				persist()
				statusView.SetText("")
			}
			closeEdit()
		})

		box := tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(input, 1, 0, true)
		box.SetBorder(true).SetTitle(" Edit value — Enter: save, Esc: cancel ")

		overlay := tview.NewFlex().
			AddItem(nil, 0, 1, false).
			AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
				AddItem(nil, 0, 1, false).
				AddItem(box, 3, 0, true).
				AddItem(nil, 0, 1, false), 0, 3, true).
			AddItem(nil, 0, 1, false)

		rootPages.AddPage("edit", overlay, true, true)
		tuiApp.SetFocus(input)
	}

	// Pick a source driver, then edit its default URL.
	showAddSourceTypeDropdown = func() {
		driverList := capture.ListRegistered()
		if len(driverList) == 0 {
			statusView.SetText("[red]No Source driver registered (check internal/drivers/drivers.go)[-]")
			return
		}

		options := make([]string, len(driverList))
		for i, d := range driverList {
			options[i] = d.Metadata.DisplayName
		}

		closeDropdown := func() {
			rootPages.RemovePage("addSourceType")
			tuiApp.SetFocus(table)
		}

		dropdown := tview.NewDropDown().
			SetLabel("Choose data source type: ").
			SetOptions(options, func(text string, index int) {
				if index < 0 || index >= len(driverList) {
					closeDropdown()
					return
				}
				chosen := driverList[index]

				cfg.Provider.Source.Type = chosen.Type
				cfg.Provider.Source.URL = chosen.Metadata.URLTemplate
				if cfg.Provider.Source.Name == "" {
					cfg.Provider.Source.Name = fmt.Sprintf("%s_source", chosen.Type)
				}
				sourceCheck.status = "unchecked"
				sourceCheck.errMsg = ""

				closeDropdown()
				rebuildTable()
				// Source URL is first row after picking it.
				table.Select(0, 0)
				startEdit(0)
			})

		dropdown.SetDoneFunc(func(key tcell.Key) {
			if key == tcell.KeyEscape {
				closeDropdown()
			}
		})

		box := tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(dropdown, 1, 0, true)
		box.SetBorder(true).SetTitle(" Choose Source type — Enter: select, Esc: cancel ")

		overlay := tview.NewFlex().
			AddItem(nil, 0, 1, false).
			AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
				AddItem(nil, 0, 1, false).
				AddItem(box, 3, 0, true).
				AddItem(nil, 0, 1, false), 0, 3, true).
			AddItem(nil, 0, 1, false)

		rootPages.AddPage("addSourceType", overlay, true, true)
		tuiApp.SetFocus(dropdown)
	}

	// Add a destination from the registered drivers.
	showAddSinkTypeDropdown = func() {
		driverList := sinks.ListRegistered()
		if len(driverList) == 0 {
			statusView.SetText("[red]No Sink driver registered (check internal/drivers/drivers.go)[-]")
			return
		}

		options := make([]string, len(driverList))
		for i, d := range driverList {
			options[i] = d.Metadata.DisplayName
		}

		closeDropdown := func() {
			rootPages.RemovePage("addSinkType")
			tuiApp.SetFocus(table)
		}

		dropdown := tview.NewDropDown().
			SetLabel("Choose destination database type: ").
			SetOptions(options, func(text string, index int) {
				if index < 0 || index >= len(driverList) {
					closeDropdown()
					return
				}
				chosen := driverList[index]

				n := len(cfg.Consumers.List) + 1
				cfg.Consumers.List = append(cfg.Consumers.List, config.DBConnection{
					Name:     fmt.Sprintf("%s_dest_%d", chosen.Type, n),
					Type:     chosen.Type,
					URL:      chosen.Metadata.URLTemplate,
					IsActive: true,
				})
				consumerChecks = append(consumerChecks, &checkState{status: "unchecked"})
				newDestIdx := len(cfg.Consumers.List) - 1

				closeDropdown()
				rebuildTable()

				// Destinations use three rows; source uses one or three.
				sourceRowCount := 1
				if cfg.Provider.Source.Type != "" {
					sourceRowCount = 3
				}
				targetRow := sourceRowCount + newDestIdx*3

				table.Select(targetRow, 0)
				startEdit(targetRow)
			})

		dropdown.SetDoneFunc(func(key tcell.Key) {
			if key == tcell.KeyEscape {
				closeDropdown()
			}
		})

		box := tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(dropdown, 1, 0, true)
		box.SetBorder(true).SetTitle(" Choose Sink type — Enter: select, Esc: cancel ")

		overlay := tview.NewFlex().
			AddItem(nil, 0, 1, false).
			AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
				AddItem(nil, 0, 1, false).
				AddItem(box, 3, 0, true).
				AddItem(nil, 0, 1, false), 0, 3, true).
			AddItem(nil, 0, 1, false)

		rootPages.AddPage("addSinkType", overlay, true, true)
		tuiApp.SetFocus(dropdown)
	}

	table.SetSelectedFunc(func(row, col int) {
		startEdit(row)
	})

	var lastClickTime time.Time
	lastClickRow := -1
	table.SetMouseCapture(func(action tview.MouseAction, event *tcell.EventMouse) (tview.MouseAction, *tcell.EventMouse) {
		if action == tview.MouseLeftClick {
			row, _ := table.GetSelection()
			now := time.Now()
			if row == lastClickRow && now.Sub(lastClickTime) < 400*time.Millisecond {
				startEdit(row)
				lastClickRow = -1
			} else {
				lastClickRow = row
				lastClickTime = now
			}
		}
		return action, event
	})

	rebuildTable()

	buttonBar := tview.NewForm().SetButtonsAlign(tview.AlignLeft)
	buttonBar.AddButton("Copy selected row", copySelected)

	// Manual holds values. Automatic can tune them while CDC runs.
	var updateModeButtons func()

	buttonBar.AddButton("Manual", func() {
		if locked {
			return
		}
		cfg.Tuning.Mode = config.TuningModeManual
		persist()
		updateModeButtons()
		statusView.SetText("[green]Mode: Manual — AutoTuner will be locked while CDC runs[-]")
	})
	manualBtnIdx := buttonBar.GetButtonCount() - 1

	buttonBar.AddButton("Automatic", func() {
		if locked {
			return
		}
		cfg.Tuning.Mode = config.TuningModeAutomatic
		persist()
		updateModeButtons()
		statusView.SetText("[green]Mode: Automatic — AutoTuner will be activated while CDC runs[-]")
	})
	autoBtnIdx := buttonBar.GetButtonCount() - 1

	updateModeButtons = func() {
		if btn := buttonBar.GetButton(manualBtnIdx); btn != nil {
			if cfg.Tuning.IsAutomatic() {
				btn.SetLabel("○ Manual")
			} else {
				btn.SetLabel("● Manual")
			}
		}
		if btn := buttonBar.GetButton(autoBtnIdx); btn != nil {
			if cfg.Tuning.IsAutomatic() {
				btn.SetLabel("● Automatic")
			} else {
				btn.SetLabel("○ Automatic")
			}
		}
	}
	// Match the loaded mode (manual by default).
	updateModeButtons()

	running := false
	const runButtonLabel = "Run CDC"
	buttonBar.AddButton(runButtonLabel, func() {
		if running {
			return
		}

		// Need a source and at least one destination.
		if cfg.Provider.Source.Type == "" {
			statusView.SetText("[red]No data source selected[-]")
			return
		}
		activeCount := 0
		for _, c := range cfg.Consumers.List {
			if c.IsActive && c.Type != "" {
				activeCount++
			}
		}
		if activeCount == 0 {
			statusView.SetText("[red]At least 1 destination is required[-]")
			return
		}

		// All selected connections needs a successful check.
		if sourceCheck.status != "ok" {
			statusView.SetText("[red]Data source connection has not been checked successfully[-]")
			return
		}
		for i, c := range cfg.Consumers.List {
			if c.IsActive && c.Type != "" && (i >= len(consumerChecks) || consumerChecks[i].status != "ok") {
				statusView.SetText(fmt.Sprintf("[red]Destination %d (%s) connection has not been checked successfully[-]", i+1, c.Name))
				return
			}
		}

		running = true
		if btn := buttonBar.GetButton(buttonBar.GetButtonIndex(runButtonLabel)); btn != nil {
			btn.SetLabel("Starting...")
		}
		runCallback()
	})
	buttonBar.AddButton("Quit", func() {
		tuiApp.Stop()
	})

	mainLayout := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(table, 0, 1, true).
		AddItem(buttonBar, 3, 0, false).
		AddItem(statusView, 1, 0, false)

	rootPages.AddPage("main", mainLayout, true, true)

	lock := func() {
		locked = true
		table.SetTitle(" Configuration (locked) ")
	}

	return rootPages, lock
}