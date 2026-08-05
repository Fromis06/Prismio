package cli

import (
	"fmt"
	"strconv"
	"time"

	"my-cdc/internal/config"

	"github.com/atotto/clipboard"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type configRow struct {
	Label    string
	Get      func() string
	Validate func(newVal string) error
	Set      func(newVal string)
	IsAction bool
	OnAction func()
}

func NewConfigForm(tuiApp *tview.Application, cfg *config.AppConfig, configPath string, runCallback func()) (tview.Primitive, func()) {
	table := tview.NewTable().SetSelectable(true, false)
	table.SetBorder(true).SetTitle(" Configuration ")

	statusView := tview.NewTextView().SetDynamicColors(true)

	locked := false
	var rows []configRow

	var buildRows func()
	var rebuildTable func()
	var startEdit func(rowIdx int)

	// Hàm persist tự tạo để ghi đè config hiện tại xuống file yaml
	persist := func() {
		if saveErr := config.SaveFullConfig(configPath, cfg); saveErr != nil {
			statusView.SetText(fmt.Sprintf("[red]Lưu config thất bại: %v[-]", saveErr))
		}
	}

	buildRows = func() {
		newRows := []configRow{
			{
				Label: "Source URL",
				Get:   func() string { return cfg.Provider.Source.URL },
				Set:   func(v string) { cfg.Provider.Source.URL = v },
			},
		}

		for i := range cfg.Consumers.List {
			idx := i
			label := "Destination URL"
			if idx > 0 {
				label = fmt.Sprintf("Destination URL %d", idx+1)
			}
			newRows = append(newRows, configRow{
				Label: label,
				Get:   func() string { return cfg.Consumers.List[idx].URL },
				Set:   func(v string) { cfg.Consumers.List[idx].URL = v },
			})
		}

		newRows = append(newRows, configRow{
			Label:    "     →  Thêm đích đến mới",
			IsAction: true,
			Get:      func() string { return "" },
			OnAction: func() {
				if locked {
					return
				}
				n := len(cfg.Consumers.List) + 1
				cfg.Consumers.List = append(cfg.Consumers.List, config.DBConnection{
					Name:     fmt.Sprintf("postgres_dest_%d", n),
					Type:     "postgres",
					URL:      "",
					IsActive: true,
				})
				newDestRowIdx := len(cfg.Consumers.List)
				rebuildTable()
				table.Select(newDestRowIdx, 0)
				startEdit(newDestRowIdx)
			},
		})

		newRows = append(newRows,
			configRow{
				Label: "Worker Count",
				Get:   func() string { return strconv.Itoa(int(cfg.DataProcessing.DataProcessingWorkerCount.Load())) },
				Validate: func(v string) error {
					n, err := strconv.ParseInt(v, 10, 32)
					if err != nil || n <= 0 {
						return fmt.Errorf("Worker Count phải là số nguyên dương")
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
						return fmt.Errorf("Batch Size phải là số nguyên dương")
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
						return fmt.Errorf("Batch Timeout phải là số nguyên dương")
					}
					return nil
				},
				Set: func(v string) {
					n, _ := strconv.ParseInt(v, 10, 64)
					cfg.Batch.BatchTimeout.Store(n)
				},
			},
		)

		rows = newRows
	}

	redrawRow := func(i int) {
		r := rows[i]
		labelColor := tcell.ColorYellow
		valueText := r.Get()
		if r.IsAction {
			labelColor = tcell.ColorAqua
			valueText = "(Enter / double-click)"
		}
		table.SetCell(i, 0, tview.NewTableCell(r.Label).
			SetTextColor(labelColor).
			SetSelectable(true).
			SetExpansion(1))
		table.SetCell(i, 1, tview.NewTableCell(valueText).
			SetTextColor(tcell.ColorWhite).
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
			statusView.SetText("[yellow]Dòng này không có giá trị để copy[-]")
			return
		}
		val := rows[row].Get()
		if err := clipboard.WriteAll(val); err != nil {
			statusView.SetText(fmt.Sprintf("[red]Copy thất bại: %v[-]", err))
			return
		}
		statusView.SetText(fmt.Sprintf("[green]Đã copy: %s[-]", rows[row].Label))
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
				redrawRow(rowIdx)
				persist() // Đã có hàm persist() xử lý
				statusView.SetText("")
			}
			closeEdit()
		})

		box := tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(input, 1, 0, true)
		box.SetBorder(true).SetTitle(" Sửa giá trị — Enter: lưu, Esc: huỷ ")

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
	buttonBar.AddButton("Copy dòng đang chọn", copySelected)

	running := false
	const runButtonLabel = "Run CDC"
	buttonBar.AddButton(runButtonLabel, func() {
		if running {
			return
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