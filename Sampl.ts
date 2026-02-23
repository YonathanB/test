// registration-page.component.ts
import { Component, inject, ViewChildren, type QueryList, signal } from '@angular/core';
import { JsonPipe } from '@angular/common';
import { MatButtonModule } from '@angular/material/button';
import { 
  DynamicFormComponent, 
  DynamicFieldTemplateDirective, 
  type FormConfig 
} from '@ma-lib/dynamic-form';

import { LocationService } from './location.service';
import { MyCustomAutocompleteComponent } from './my-custom-autocomplete.component';

@Component({
  selector: 'app-registration-page',
  standalone: true,
  imports: [
    DynamicFormComponent, 
    DynamicFieldTemplateDirective, 
    MyCustomAutocompleteComponent,
    MatButtonModule,
    JsonPipe
  ],
  template: `
    <div class="page-container" style="max-width: 800px; margin: 2rem auto; display: flex; gap: 2rem;">
      
      <div class="fragment" style="flex: 1;">
        <lib-dynamic-form 
          [config]="personalConfig" 
          (formValueChange)="onFragmentChange('personal', $event)">
        </lib-dynamic-form>
      </div>

      <div class="fragment" style="flex: 1;">
        <lib-dynamic-form 
          [config]="locationConfig" 
          (formValueChange)="onFragmentChange('location', $event)">
          
          <ng-template dynamicFieldTemplate="cityAutoTpl" let-field let-control="control" let-dataSource="dataSource">
            <label class="mat-caption">{{ field.label }}</label>
            <app-my-custom-autocomplete 
              [control]="control" 
              [provider]="dataSource">
            </app-my-custom-autocomplete>
          </ng-template>

        </lib-dynamic-form>
      </div>

    </div>

    <div class="global-actions" style="max-width: 800px; margin: 2rem auto; text-align: right;">
      <button mat-flat-button color="primary" (click)="submitAll()">
        Valider l'inscription
      </button>
    </div>

    <pre style="max-width: 800px; margin: 2rem auto; background: #eee; padding: 1rem;">
      {{ globalState() | json }}
    </pre>
  `
})
export class RegistrationPageComponent {
  private locationService = inject(LocationService);

  @ViewChildren(DynamicFormComponent) forms!: QueryList<DynamicFormComponent>;

  globalState = signal<Record<string, any>>({});

  // ─── CONFIGURATION 1 : Infos Personnelles ───
  personalConfig: FormConfig = {
    id: 'personal',
    title: 'Contact',
    hideActions: true, // Désactive les boutons internes
    fields: [
      {
        key: 'contactMethod',
        type: 'select',
        label: 'Méthode de contact préférée',
        options: [
          { label: 'Email', value: 'email' },
          { label: 'Téléphone', value: 'phone' }
        ]
      },
      {
        key: 'contactValue',
        type: 'text', // Type par défaut (invisible)
        hidden: true,
        dependencies: [
          {
            // Bascule en type email
            conditions: [{ field: 'contactMethod', operator: 'equals', value: 'email' }],
            effects: { visible: true, type: 'email', label: 'Adresse e-mail', required: true, resetValue: true }
          },
          {
            // Bascule en type text (téléphone)
            conditions: [{ field: 'contactMethod', operator: 'equals', value: 'phone' }],
            effects: { visible: true, type: 'text', label: 'Numéro de mobile', required: true, resetValue: true }
          }
        ]
      }
    ]
  };

  // ─── CONFIGURATION 2 : Localisation ───
  locationConfig: FormConfig = {
    id: 'location',
    title: 'Localisation',
    hideActions: true,
    fields: [
      {
        key: 'country',
        type: 'select',
        label: 'Pays',
        options: this.locationService.countries // Connexion directe au Signal
      },
      {
        key: 'city',
        type: 'custom',
        label: 'Ville de résidence',
        customTemplateRef: 'cityAutoTpl', // Lie ce champ au <ng-template> HTML
        disabled: true,
        dependencies: [
          {
            conditions: [{ field: 'country', operator: 'isNotEmpty' }],
            effects: {
              disabled: false,
              resetValue: true,
              // Injection dynamique du provider en fonction du pays choisi
              dataSource: (formValues) => ({
                getData: (txt: string) => this.locationService.searchCities(formValues['country'], txt)
              })
            }
          }
        ]
      }
    ]
  };

  // ─── LOGIQUE D'ORCHESTRATION ───

  onFragmentChange(fragmentName: string, values: Record<string, any>) {
    this.globalState.update(state => ({ ...state, ...values }));
  }

  submitAll() {
    // Vérification de tous les fragments
    const allValid = this.forms.toArray().every(form => form.getFormGroup().valid);

    if (!allValid) {
      // Touche tous les champs pour faire apparaître les erreurs Material (mat-error)
      this.forms.forEach(form => form.getFormGroup().markAllAsTouched());
      console.warn("Le formulaire global est invalide.");
      return;
    }

    const finalPayload = this.globalState();
    console.log('Soumission API réussie :', finalPayload);
    // ex: this.http.post('/api/users', finalPayload).subscribe(...);
  }
}
